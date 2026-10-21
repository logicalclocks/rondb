---
name: rondb-978-port
description: "RONDB-978 user rate limits port onto branch RONDB-978-port — status, decisions, conflict resolutions, next steps"
metadata: 
  node_type: memory
  type: project
  originSessionId: 570eebae-4926-405b-aecb-24fc1479f24b
---

# RONDB-978 "User Rate Limits" — Port Handoff

## GOAL (final, user decision 2026-07-27)

**Stay based on 26.02-main.** (Earlier that day user considered 26.05-main as target;
after seeing the ring-buffer bit collisions below, chose to continue on 26.02-main.
The 26.05 blocker table remains relevant IF a 26.05 port is attempted later.)

## Status update (2026-07-27, end of session work)

- **Pushed**: `origin/RONDB-978-port` (origin = smkniazi fork, pushed 10:10) =
  `050cf0cb874` (= current upstream/26.02-main TIP, 0 commits behind) + exactly
  **11 commits** ("Fix version header" dropped by rebase — "patch contents already
  upstream"). NOT a cherry-pick of RONDB-1090 on the branch — `46df7c1f12b` is simply
  in 26.02-main's merge history below the base (earlier note claiming a branch
  cherry-pick was WRONG).
- **Local rebase redone by Claude on user request** (user's own attempt hit conflicts,
  aborted): `git rebase remotes/upstream/26.02-main` on the old 12-commit branch;
  4 conflicts in commit #1 only (TcKeyRef.hpp, ndb_version.h.in, NdbTransaction.hpp,
  NdbTransactionScan.cpp) resolved by taking origin core commit `3b9ead12e31`'s blobs
  (safe: identical base for those paths). Result verified: range-diff all 11 `=`,
  **tree hash identical** to origin (`90a4d6ce75e0...`); then reset branch to origin's
  commits to avoid hash divergence. Local branch now == origin/RONDB-978-port exactly.
- **Merges CLEANLY into 26.02-main** (it IS 26.02-main tip + 11). A PR → 26.02-main is
  the natural next step (needs user go-ahead).
- **MTR PASSED on the new base too** (136.9s; user rebuilt with make -j4 after the
  unlimited-parallelism incident, see [[build-parallelism-limit]]).

## Pre-existing 26.02-main bug found & fixed (UNCOMMITTED as of session end)

First MTR run on the new base crashed: debug mysqld asserts at startup —
`ndbinfo_define_dd_tables` (ha_ndbinfo_sql.cc:1050) requires `views[]` strictly
alphabetically sorted, but upstream commit `0cc2a3bc87b` ("Add runtime control and
ndbinfo monitoring for RdmaLogLevel") APPENDED `rdma_transporters` at the array end.
Broken on 26.02-main tip itself (any debug build); release CI compiles the assert out.
Fix: moved the 2-line entry between `processes` and `resources` in
`storage/ndb/plugin/ha_ndbinfo_sql.cc`; verified 64 views sorted; `lookups[]` was fine.
**Sitting uncommitted in the working tree — user must decide: extra commit on
RONDB-978-port vs separate PR to 26.02-main** (upstream needs it either way).
- **MTR PASSED locally**: `ndb_quota.ndb_rate_limits` [pass] 137s. The old "blocked on
  macOS" verdict was wrong — crash was the Claude sandbox ([[macos-mysqld-startup-crash]]).
  Ran unsandboxed with `--vardir=/tmp/mtr978var --port-base=16000` to not disturb the
  live seeded RDRS cluster in build/mysql-test/var.
- **No PR yet** (`gh pr list --head RONDB-978-port --state all` → empty).

## ✅ 26.05 upmerge done 2026-07-30 — REDONE per Mikael's rule (lower version wins)

Merge commit `f6919f81b78` (amended: 00efe92bc1f → fa5d07cdccf → 4de1c3dca11 → final,
last amend = 31 copyright start-year restorations per Mikael, e.g. 2021, 2026 not
2024, 2026 — user chose amend over separate commit) on branch
`upmerge-26.02.9-into-26.05-main` in `/Users/salman/code/hops/rondb2`
(26.05-main tip `0ee5cbaf98e` + upstream/26.02-main tip `b5336b8978b` = PR #1015).
Mikael CONFIRMED the bit approach + new RB bits OK to change freely (only 26.02 is
released; 26.04.x are betas w/o upgrade support). ndbd_support_user_rate_limits
25.10.2 claim stays.

**⚠️ CANONICAL RULE (Mikael, Slack 2026-07-30): on any conflict of error codes, GSN
numbers, or bit positions between versions, THE LOWER VERSION ALWAYS WINS.** My first
resolution adopted 26.04-main's layout (userId TcKeyReq 31 / ScanTab 0 / ScanFrag 30 /
LqhKey attrLen 30) — Mikael rejected it. Final layout in the merge:

| Signal    | UserId (26.02, frozen)  | Ring buffer (moved, NEW bits)          |
|-----------|-------------------------|----------------------------------------|
| TcKeyReq  | requestInfo 29          | ShowMeta 30 (unchanged), Op 29→**31**  |
| ScanTab   | requestInfo 5           | ShowMeta 5→**0**                       |
| ScanFrag  | requestInfo 27          | ShowMeta 27→**31**                     |
| LqhKeyReq | requestInfo 9           | ShowMeta 4 (unchanged), Op RI9→**attrLen/SI bit 31** (requestInfo is full) |

New ring-buffer positions are MY choices — need Mikael's sign-off; 26.04-main (userId
31/0/30/SI30 + RB on old bits) violates the rule the same way and needs the mirror
fix upstream (Mikael's call). LqhKey RB-Op word change touched: restore.cpp:3068
(req->attrLen), DbtcMain ~5627 + DblqhMain ~12687 (flag set on lqhKeyReq->attrLen
AFTER attrLen is assigned — setting earlier would be overwritten), DblqhMain 9246
reader (attrLenFlags). getLongClearBits deleted (zero callers; its "undefined bits"
9/4 mask became meaningless once 9 = userId). All 10 LqhKey userId call sites are
26.02's requestInfo form (auto-merge's original output was correct all along).

Also in the merge: `ndb$error_messages` = **893** in ndbinfo_plans.result by delta
arithmetic (Mikael's Jul-28/29 recordings pin actual(base)=889; +940+942+2203+mgm1018);
both parents' committed counts look stale — VERIFY by running the test. Build/MTR not
run since the bit redo. User had pushed the earlier version — needs FORCE PUSH.

## ✅ 26.05-main → 26.04-main merge DONE 2026-07-30

Merge commit `054baa3d0a4` on `upmerge-26.05-to-26.04` in rondb2/ (26.04-main tip
`5a188b40ac1` + upstream/26.05-main tip `b01171231c9` = PR #1023). 96 conflicts.
Key resolutions (all per Mikael's lower-version-wins rule):
- **Bits**: userId reverted to 26.02 positions in all 4 headers (26.04 had 31/0/30/
  SI30); ring buffer moved to approved new homes (TcKeyReq Op 31, ScanTab SM 0,
  ScanFrag SM 31, LqhKey Op SI 31); 26.04's JoinAgg bits kept (ScanTab 1, ScanFrag
  28/29, LqhKey SI 28/29) — no collisions. Auto-merge left DUPLICATE defines
  (USER_ID_SHIFT, SCAN_USER_ID_SHIFT, ScanTab setUserIdFlag body) — removed.
- **Error codes**: 942 = DatabaseNameTooLong (26.02 wins); 26.04's TooLargeDiskSize
  Part moved 942→943; its "Database name too long"@943 dropped; ndb_dd_basic.result
  + ndb_quota_commands.result updated; ndberror gets 941/942/943/944 + 1243/1244.
- **Config IDs**: 704/705 = RestartBarrierTimeout/GracefulShutdownTimeout (26.02
  wins); 26.04's JOIN_AGG_STATE_POOL_SIZE→706, ENABLE_PROACTIVE_DEADLOCK_DETECTION
  →707.
- **rdrs2**: per user, took DOWNSTREAM (26.02/26.05) wholesale for all 65 conflicts;
  46 modify/delete fixture files deleted (user approved via prompt; old server/test/
  resources/testdbs tree + old test_go hopsworks_31/33/34 fixtures). TRAP: bulk
  `git checkout --theirs <dir>` SILENTLY SKIPS files after delete/modify errors —
  19 files got staged with conflict markers; caught by grep sweep, re-fixed via
  `git show $T:$f`. ALWAYS grep '<<<<<<<' the whole tree before committing a merge.
- **Semantic picks**: kept 26.04's execTCKEYREQ user-version-mismatch abort (vs
  26.02's ndbassert(false)); kept 26.04 LQHKEY_error_cold; combined DIH_SCAN_TAB_
  CONF = 26.04 CTE guard + 26.05 fanout MIN(scanParallel,tfragCount) (RONDB-1074);
  CommandInterpreter/Services SET params union (MaxDiskWriteSpeed,
  EnableProactiveDeadlockDetection, RdmaLogLevel) + ndb_config_set.result updated;
  Services.cpp USER cmds pass real user_name (26.04 passed nullptr).
- ndbinfo_plans error_messages = **922** (26.04's recorded 919 + 1243/1244/2203),
  est_rows=actual count stays 27 (matches auto-merged list). Verify by running.
- Copyright audit: 0 start-year regressions (incoming side had PR #1023 fixes).

**Build fixes (amended into merge → `5bad04935d1`, builds 100%):** auto-merge landmines:
(1) ndb_version.h.in had ndbd_support_user_rate_limits TWICE — kept Mikael-blessed
25.10.2 gate, deleted 26.04's 26.04+ gate; (2) RonSQLPreparer ParseOnly ctor (26.05
authz) missing 26.04's new DynamicArray members — added to init list; (3)
testNodeRestart.cpp union broke — REDONE as HEAD-version + `git apply --3way` of
base→26.05 diff; the shared `/**` opener + `return NDBT_OK;}` closer must be
duplicated per side when unioning adjacent function blocks; (4) rdrs2 PORT of 26.04
features onto downstream core (user chose port over drop): CONFLICT=409 re-added to
rdrs_dal.h enum, pk_batch_delete/write DAL re-added with downstream signature
(rate_limit_identity/len instead of username_ptr; BaseBatchOperations::set_user_id
no longer concats {project}_{user} — identity arrives complete from
get_rate_limit_identity), batch_pk_delete/write_ctrl rewritten to downstream
authenticate(api_key, accessReqs) with whole-table (columns=nullptr) access +
rl_identity, health/ping/ttl_purge ctrls to 1-arg authenticate_empty,
config_structs_def.hpp re-added 26.04's MySQLRouterConfig + rondis
requireTablesOnStartup + ronsql logQueries (rate-limit config kept downstream).
**MTR results on merge `1e40007718e` (amended: 923/394 recorded in ndbinfo_plans —
my 922 arithmetic was off by one; config_values actual 394 = +4 config params):**
ndb_quota 12/12 ✅ (incl. ndb_rate_limits, ndb_quota_commands=942), ndb_ring_buffer
18/18 ✅, ndbinfo+plans 3/3 ✅, ndb_opt pushdown 5/5 ✅. rdrs2-golang: 10/11 completed
pass, **rdrs2-golang_sharing FAILS DETERMINISTICALLY (2/2 runs)** — but ONLY in the
TLS re-run (non-TLS pass exits 0); even owner_narrow_fv fails: narrow (column-
selected) feature-vector reads return status MISSING/feature <nil> on the second
(TLS) server run. 15 suite tests never ran (mtr stops at failure; incl. 26.04's new
batchwrite/batchdelete/batchcomprehensive/jsonerrors Go tests — untested against the
ported rdrs2!). fs files verified ≈ downstream (only 26.04 1M buffer + V2 route
differ) so NOT the fs chimera. **sharing ROOT CAUSE FOUND (2026-07-30): TLS is a RED HERRING.** MTR starts TWO
standalone rdrs2 servers (rdrs.1.1=19005 noTLS, rdrs.2.1=19006 TLS) at cluster start,
BEFORE the Go tests seed the fixture DBs. The FS-metadata cache EVENT WATCHER
prefetches each feature_view as its row is INSERTed mid-seed; if the preload reads
the FV's dependent metadata (features/joins/serving keys) in a partially-inserted
but non-erroring state, the incomplete entry is cached WITHOUT error → no deferred
retry (RONDB-1030's retry only triggers on preload ERROR) → that server permanently
serves status=MISSING for the affected narrow FVs (usera_txncount_fv). Whichever
server hits the window during seeding is poisoned for life; the other passes —
hence pass/fail flipping between noTLS/TLS phases across runs. PROOF: 3-phase
experiment on one cluster (noTLS seeded=FAIL, TLS=PASS, noTLS again=FAIL 30min
later, 5s api-key refresh doesn't heal FV cache); live poisoned server PID logs
/tmp/mtr2604s/log/rdrs.1.1.log lines 104-121: "INSERT detected for ...
usera_txncount_fv" with NO warning (silent partial preload) vs neighbors' "Failed
to read serving keys" warnings (error path = healed). Same design flaw exists on
26.02 (run #12's race was this class) — the 26.04/26.05 kernel timing just hits it
reliably when the sharing MTR test seeds its own fresh cluster. REAL FIX (server):
event-triggered FV preload must treat success as provisional (deferred re-verify)
or validate completeness. **FINAL RESOLUTION (2026-07-31): e7f776e1c66 DROPPED (git reset), replaced by
ATOMIC SEEDER commit (final hash `838c89a66c8`)** — user verified Hopsworks production writes
the whole FV graph in ONE cascaded JPA transaction (FeatureViewFacade @Stateless
default-REQUIRED tx; FeatureView entity cascades ALL to features/joins/filters/
servingKeys/trainingDatasets — checked in /Users/salman/code/hops/hopsworks), so
the partial-read window was a TEST-SEEDING artifact (Go seeder = autocommit per
statement). Fix: schemata.go runQueriesWithConnection does SET autocommit=0 +
single COMMIT (ROLLBACK on error) on the pinned conn → each DB's data = one NDB
tx (DDL implicitly commits; data follows DDL in fixtures; largest fixture ~1037
single-row inserts vs MaxNoOfConcurrentOperations=10000). VALIDATED: the
previously deterministic repro (fresh cluster, sharing test seeds itself) passes
2/2 with fs_cache.cpp reverted. Comment in code forbids "commit every N"
chunking (would split an FV's rows). **Cherry-pick 1f10ea80ab2 (not the old
fs_cache fix) to 26.02-main — same seeder + same flake class (run #12) there.**
The FS-cache hardening options (provisional re-verify / refresh sweep / wider
event subscriptions / schema ready-marker) remain documented for Mikael as
optional defense-in-depth against non-atomic writers (manual SQL, restores).

(historical) FV-PRELOAD FIX was briefly commit `e7f776e1c66` on
upmerge-26.05-to-26.04 (top of merge 1e40007718e): fs_cache.cpp
load_single_feature_view treats event-triggered preload success as provisional —
re-reads until two consecutive reads 500ms apart have the same shape tuple
(numOfFeatures, featureGroupFeatures, featureIndexLookup, primaryKeyMap,
complexFeatures sizes), max 6 iterations, interruptible via m_sleepLock/m_sleepCond
(m_stopped atomic). VALIDATION: fresh-cluster run showed "read changing metadata,
re-verifying" fire on BOTH servers during seeding and zero MISSING failures — but
that mtr run died from this box's known external SIGABRT (mysqltest killed in
fgets), and a retry was stopped; a clean end-to-end green sharing run is STILL
PENDING. Fix must ALSO go to 26.02-main (same code, released line — cherry-pick
e7f776e1c66) so it flows 26.02→26.05→26.04. Also verify downstream
ndbinfo_plans recordings by real runs: 26.02-main tip fails by 2 (PR #1015 didn't
re-record); 26.05's 893 (PR #1023, my arithmetic) is suspect — likely 894 (same
off-by-one as 26.04's 922→923). Remaining for upmerge PR: run the 15 unrun
rdrs2-golang tests (incl. 26.04's batchwrite/batchdelete against the ported rdrs2).
Unpushed.

## (historical) ⚠️ BLOCKER for 26.05-main: bit collisions with Ring Buffer feature

`git merge-tree upstream/26.05-main origin/RONDB-978-port` conflicts in 4 signal headers
+ ndberror.cpp. ALL FOUR are genuine bit collisions — 26.05-main's Ring Buffer feature
(PR #909) took exactly the bits the port's userId flags use:

| Signal (requestInfo)  | port userId bit | 26.05 ring-buffer bit | 26.04-main final userId |
|-----------------------|-----------------|-----------------------|-------------------------|
| TcKeyReq              | 29              | 29 (RB Op)            | **31** (`USER_ID_SHIFT`)|
| ScanTabReq            | 5               | 5 (RB ShowMeta)       | **0** (`SCAN_USER_ID_SHIFT`)|
| ScanFragReq           | 27              | 27 (RB ShowMeta)      | **30** (`SF_USER_ID_SHIFT`)|
| LqhKeyReq             | 9               | 9 (RB Op)             | **attrLen `SI_USER_ID_SHIFT=30`** (via RONDB-1049)|

Upstream 26.04-main already has BOTH features coexisting with the relocated userId bits
(right column). Ring-buffer bits are identical on 26.04 and 26.05. **For 26.04↔26.05
wire compatibility the 26.05 port MUST adopt 26.04-main's final userId bit layout** —
which for LqhKeyReq means the RONDB-1049 attrLen relocation (port RONDB-1049 fully, per
the audit warning below; never DBSPJ-reader commit `0564ea16c1c` alone).

## Status (as of 2026-06-26)

**DONE & COMMITTED.** Branch `RONDB-978-port` (off base `50706ac0bec`, a 26.02-main
descendant) holds **12 cherry-picked commits**, author = Mikael Ronström, committer =
Salman Niazi, all carrying `(cherry picked from ...)` provenance (`-x`). Working tree
clean. Source of the commits: `upstream/26.04-main`, also checked out locally at
`/Users/salman/code/hops/rondb2` (build dir `/Users/salman/code/hops/rondb2/build`).

Our working checkout: `/Users/salman/code/hops/rondb`, build dir `.../rondb/build`.

## What the feature does

Treats a "user" as a quota entity (`m_is_user`, stored `$`-prefixed) carrying ONLY
rate limits (memory/disk quotas stay database-only). Builds on the existing Database
Quota infra already present in HEAD. Threads a userId through the wire protocol
(TcKeyReq/LqhKeyReq/ScanTabReq via backward-compatible optional flags), adds a
username→userId cache in ClusterMgr, new mgm `USER` commands, and (in upstream) RDRS2
API-key→`{project}_{username}` mapping.

Version gate: `ndbd_support_user_rate_limits()` → `major > 25`. Our branch is major 26,
so it self-enables; no version-constant surgery for the 26.04→26.02 move.

## Commits ported (oldest→newest), current hashes

6e7b16d7989 core feature · f2349868745 fix test cases · 6200e30a74b fix version header ·
9116abcb296 new test case · 9becff1573f TCKEYREQ key-length not use user-id flag ·
f9dc9182506 attr name per constant · 87ca59a87e8 + 0b7dd3bd18b missing break ·
3c88f9028dc fix issues · ebad3e1eafb rate overflow errors · 2aee9f696ec rate limit
test + overload reads/writes · 8aac07009b6 only stop new txns when user limit exhausted.

## Decisions (confirmed by user)

1. **Preserve all commits** via sequential `git cherry-pick -x` ("one by one").
2. **RDRS dropped entirely** — all `storage/ndb/rest-server2/` reverted to current
   branch. Reason: api_key.cpp/.hpp diverged heavily in HEAD (RONDB-1021/1022/1030
   refactors) vs the feature's username threading; taking HEAD on 2 conflicts but
   feature on ~15 auto-merged RDRS files would not compile. **Consequence:** user rate
   limits enforced via mgm `USER` cmds + `NdbTransaction::setUserId()` + MySQL-server
   path (ha_ndbcluster.cc), NOT the REST path.
3. **Commit #12 `2b589058787` skipped** — labeled RONDB-978 but branches off RONDB-878,
   touches only DbtupMeta.cpp, zero rate-limit content; its MAX_FIXED_SIZE_IN_WORDS
   row-size checks conflict with our branch's consolidated ZTOO_LARGE_TUPLE_ERROR.
4. **Excluded out of scope:** merge commits `32220b33624` + `7ed1b3092ef` (drag in
   RONDB-878/983), and interleaved `8e2e7bd0b3b` "Fix ndbinfo_plans".

## Conflict resolutions (commit #1 only; #2–#13 applied clean)

- `ndb_version.h.in` — kept BOTH HEAD's `ndbd_support_arbitration_rank_wait` and the
  feature's `ndbd_support_user_rate_limits` (merge had mangled their shared tail).
- `DblqhMain.cpp` (2 spots) — kept HEAD's 3-arg `abortErrorLab(...)` + feature's
  `ndbabort()`; added feature's `tcPtr->m_user_ptr_i` middle arg to `update_rate_usage`.
- `ClusterMgr.cpp` — took feature's `PRINTF_FMT(1,2)` form (macro defined just above).
- `mgmapi.h` — kept feature's user fns, re-added HEAD's `ndb_mgm_print_error` after them.
- All `rest-server2/` — restored to HEAD per decision #2.
- Verified: all 5 `update_rate_usage` call sites are 3-arg; zero conflict markers.

## Verification

- Incremental build (`make -j4`): PASS (exit 0, 100%).
- Clean rebuild (`make clean && make -j4`): PASS.
- `ndb_rate_limits` MTR test: **BLOCKED** — debug mysqld SIGSEGVs at startup in
  `delegates_init()`/`Trans_delegate` (sql/ core the port never touches). Proven
  environmental: reproduces with bare `mysqld --initialize`, survives clean rebuild,
  AND reproduces identically on the untouched upstream `rondb2` (26.04.1) checkout.
  See [[macos-mysqld-startup-crash]]. Must run on Linux/CI.

## Completeness audit (2026-07-02) — are any RONDB-978 commits missing?

Audited `rondb2` by CONTENT (git pickaxe `-S`/`-G` on user-rate-limit symbols) over
`2835db7a820..HEAD`, not by label. **Verdict: 12-commit port is complete; no mislabeled
RONDB-978 fix is missing.** Beyond our 12:

- `f3d5bb8fa58` "Merge fix" — 1 line adding `m_user_ptr_i` to `update_rate_usage` in
  `continueACCKEYREF`. **Already in our port** (DblqhMain.cpp:14873, hand-resolved in
  commit #1). Upstream needed it due to an evil merge (PR #813) that dropped the arg.
- RONDB-1049 series (May 2026, PR #935): `251fec44576` `21c68e4b327` `25a35e7801c`
  `0564ea16c1c` `70662e46e97`. Separate security feature that relocated the LqhKeyReq
  userId flag `requestInfo`(RI_USER_ID_SHIFT=9) → `attrLen`(SI_USER_ID_SHIFT). NOT
  RONDB-978. ⚠️ Do NOT cherry-pick `0564ea16c1c` alone — DBSPJ would read `attrLen`
  while our senders write `requestInfo`, breaking rate limiting on SPJ paths.
- `d133169c7d8` RONDB-1022 — API-key cache preloading (RDRS, dropped).

Our port is self-consistent: all `set/getUserIdFlag` sites use `requestInfo` bit 9.

## Next steps (as of 2026-07-27)

- [x] Push `RONDB-978-port` — done by user 2026-07-27 (rebased onto 050cf0cb874).
- [x] Run `ndb_rate_limits` MTR — PASSED locally (unsandboxed, isolated vardir).
- [ ] **Resolve ring-buffer bit collisions before any merge to 26.05-main** — adopt
      26.04-main's userId bit layout (see blocker table above); requires porting the
      RONDB-1049 LqhKey attrLen relocation as part of it.
- [ ] Open PR → base `26.05-main` (outward-facing — needs user go-ahead).
- [ ] (Optional) revisit whether RDRS REST-path user rate limiting should be re-added
      against HEAD's current api_key.cpp structure.


## FINAL PR STATE (2026-07-31, double-checked)
Branch upmerge-26.05-to-26.04 FINAL: merge `bfd13303c11` + seeder `f12caea11d7`. AUDIT RESULT: rdrs_hopsworks_dal.cpp wholesale-take had dropped RONDB-996/997 fix (af003f9dc5e: 2 assert(col_size)→graceful RS_SERVER_ERROR on wrong-charset hopsworks schema) — RESTORED into merge. api_key.cpp drops verified = only old username_ptr authenticate signatures (superseded by RONDB-1088 grants authz). pkr_operation drops = RONDB-996 base-class refactor; 26.04 base-class files coexist with downstream monolithic read path (duplication, not loss — flag in PR). The merge
absorbed two late fixes found by the --force full-suite run + audits: (1) jsonerrors:
wholesale-downstream had dropped 26.04's 11 setErrorResponse calls in
batch_pk_read_ctrl.cpp (JSON error envelope for V2 batch endpoints, error_response.hpp
isJsonErrorVersion) — ported onto downstream body (12 sites), validated green;
(2) 8 copyright start-year regressions vs 26.04 in wholesale rdrs2 files (2024→2023
restored). Verified on final history: 0 conflict markers, bit layout correct
(userId 29/5/27/RI9, RB 31/0/31/SI31+RI4), err 942/943, cfg 706/707, version fn ×1,
copyright regressions 0 vs BOTH parents. MTR: ndb_quota 12/12, ring_buffer 18/18,
ndbinfo 3/3, ndb_opt 5/5, rdrs2-golang 26/26 (jsonerrors+sharing fixed). OPEN: 
(a) function-level classification of remaining dropped 26.04 hunks in
rdrs_hopsworks_dal.cpp (~123 non-ratelimit-looking lines; sample = api-key prefix
schema check — api-key tests pass, likely equivalent downstream forms) + api_key.cpp;
(b) one final full-suite run on final tree for a single clean 26/26 record;
(c) LESSON: wholesale file-level --theirs is lossy — always diff result vs the
discarded side and justify every dropped hunk (jsonerrors class escapes builds).
Unpushed.