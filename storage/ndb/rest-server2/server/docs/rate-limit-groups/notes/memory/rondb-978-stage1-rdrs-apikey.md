---
name: rondb-978-stage1-rdrs-apikey
description: "Stage 1 RDRS API-key rate limiting: implementation status + the growing list of latent RONDB-978 feature bugs found (mgm crash, is_user lost, ClusterMgr threading, enforcement inert)"
metadata: 
  node_type: memory
  type: project
  originSessionId: be5aab2d-8dec-443f-97ee-fd939a2ea385
  modified: 2026-09-23T14:15:15.754Z
---

# RONDB-978 Stage 1: RDRS API-key rate limiting (branch RONDB-978-port)

## FINAL OUTCOME (verified against git 2026-09-23): apikey mode MERGED, username mode HELD BACK
On 2026-08-18 the branch was SPLIT: a filtered copy `RONDB-978-API-KEY-Cont` (every
username-identity commit stripped) is what reached **upstream/26.02-main** (via the 25.10→26.02
upmerge; rebadged hashes there: 1651eacc4f6 .RateLimit section, 4880413b1d4 RonSQL 429,
a99bc88b752 RonSQL tagging, 563ae360e8d/931ad525173 pooled-state clears, 0bbd8da24b7 SQL
throttle test, 133e5d67403 mgm USER family fix, 7a5c4f9559d authoritative cache re-LIST).
**Username mode (aab9edbfbaf) is NOT upstream** — it sits only on origin/RONDB-978-Cont
(fork; tip 99eb63a9a0c 2026-08-18 incl. shared-FS billing fix 1bc7a4846ca + design-doc
corrections). The API-KEY-Cont branch itself is not fetched in this checkout.

**Why username mode was held back** (per design doc §7/§9/§10 on RONDB-978-Cont at
storage/ndb/rest-server2/server/docs/RONDB-978_username_rate_limit_design.html — the
authoritative "why not yet" record; the July/Aug transcripts are pruned):
1. **Inert without Hopsworks-side provisioning** — kernel meters only $-prefixed USER
   entities; unprovisioned = unmetered. Needs USER SET/DROP co-located with
   OnlineFeaturestoreFacade MySQL-account lifecycle (or operation_log async handler) +
   service-user exemption + reconciliation job. Cross-repo dependency on hops/hopsworks.
2. **Open product decisions (§10)**: billing direct pk-reads/scans on a shared project's
   db the caller isn't a member of; mixed-db batches (first-op-wins shipped as stopgap
   vs per-project txn groups); JWT-request identity mapping.
3. **Requires UseHopsworksAPIKeys=on** — config validation rejects "username" otherwise
   (username half comes from hopsworks.users.username via the api-key cache); plain
   RonDB deployments can't use it.
4. **Rollout plan (§9) said ship dormant**: apikey default → ship Hopsworks provisioning
   → flip per cluster with rolling restart. Splitting let uncontroversial fixes merge
   without waiting on review of the larger username change (authenticate() return types
   + api-key cache = big review surface; doc also post-mortems the broken original
   rondb2 {project}_{user} concatenation the port removed).
Resuming = mostly Hopsworks work, not RonDB; code+tests already on RONDB-978-Cont.
Identity must stay byte-identical to ha_ndbcluster::set_user_id's bare account name or
REST and SQL won't share a kernel bucket.

## Decisions (user)
429 for throttled; RonSQL deferred to last; identity = API-key PREFIX by default but
full key configurable; test limits must be easy/deterministic (tune empirically);
rate limiting (apikey) enabled by default for all Go tests; implement autonomously.

## Implemented (compiles; RDRS side verified live)
- Config (config_structs_def.hpp REST): UserRateLimits(false), RateLimitIdentity("apikey",
  validated), RateLimitFullAPIKey(false). Mirrored in test_go config structs/defaults.
  MTR template rdrs_config_template.json sets UserRateLimits=true for the suite.
- rate_limit.hpp: get_rate_limit_identity(api_key) → prefix (or full key per config).
- Threading: pk_batch_read + scan_read take (rate_limit_identity, len); 5 controllers pass
  it; BatchKeyOperations::set_transaction_user_id() after each startTransaction (2 sites);
  scan tagging in perform_scan after TransactionGuard.
- 429 mapping: HTTP_CODE TOO_MANY_REQUESTS=429; __RS_ERROR_RONDB maps NDB 243/2203/247/248;
  scan ctrl propagates 429 (was hardcoded 400); TranslateRonDbError + RATE_LIMIT_EXCEEDED(20)
  for feature-store endpoints. CanRetryOperation does NOT retry OL errors (verified).
- Go: internal/mgmclient (MGM text protocol: set/alter/drop/get user; reply "result: Ok");
  testutils/ratelimits.go (APIKeyPrefix, SetOrAlterRateLimitUser,
  ProvisionDefaultRateLimitUsers with HIGH_RATE_LIMIT=1000000, hooked in InitialiseTesting)
  + the SHARED rate-limit harness: SkipIfRateLimitsDisabled, RunEndpointRateLimitTest,
  RunZeroRateIsUnlimitedTest (closure-based: endpoint passes `sendOne func(*http.Client) int`,
  harness owns identity/mgm-limit/burst[400@parallel15,settle2500ms]/assert; closure avoids
  the testclient→testutils import cycle; burst shares ONE pooled *http.Client).

- **FULL ROOT CAUSE (2026-07-29, MTR "flaky" enforcement — TWO stacked causes)**:
  (1) USER-ID CACHE BLINDNESS, per-RDRS-process coin flip: retrieveUserId treated a miss
  after m_initialised as "no user, forever"; the GET_DATABASE fallback (only ran while
  initialising) resolved the id but receiveGET_DATABASE_CONF DISCARDED it because
  updateUserId was update-only (-1 when no entry — placeholder inserts had been removed);
  and the push healer CREATE/DROP_DATABASE_REP garbled usernames (memmove(&dbName[0],
  &dbName[1]) on a Uint32[] shifts 4 BYTES not 1 char — LIST path was char-correct).
  Net: an RDRS whose cache initialised without the $user could NEVER tag → zero enforcement
  for process lifetime; restart re-rolls. PROOF: restarting rdrs.1.1 on a blind cluster
  instantly restored throttling. FIXES (uncommitted): updateUserId → upsert;
  retrieveUserId miss → always return 1 (GET fallback, self-healing, unprovisioned users
  pay one GET per txn); Dbdict both REPs char-based strip + (len+4)/4 section size.
  (2) THRESHOLD DYNAMICS: enforcement flags latch only when queueing delay from usage
  accumulated over 100ms ticks crosses read-abort ~30ms; usage decays per tick, so
  fixed-count bursts on a warm system finish merely DELAYED (batchFS peaked 20.5ms with
  400×15-par ≈ never rejected; 800×64-par even faster ≈ worse). One kernel 429 then arms the
  ~1s client backoff which cascades 429s. FIX: testutils burst is duration/outcome-bound
  (32 parallel senders loop until first 429 or 30s; zero-rate test 3s fixed) — never
  count-bound. Priming waves DON'T work (usage decays during any settle).
  DIAGNOSTIC GOLD: DEB_RATE*/DEB_QUOTAS compiled in → ndbd.log has per-100ms
  "(inst):dbId overload: X, queueing_time: Y", per-op ":dbId INC api_ref_count",
  "dbNamePtr:" per GET_DATABASE, "DBTC/DBLQH Alter Database id" per alter distribution.
  (3) EPHEMERAL PORT EXHAUSTION: duration-bursts with the default http.Transport
  (MaxIdleConnsPerHost=2!) churned thousands of TIME_WAIT sockets → "dial tcp
  0.0.0.0:13005: can't assign requested address" → goroutine t.Fatalf → "0 ok 0 limited"
  transient failures. FIX: setupBurstHttpClient with MaxIdleConns[PerHost]=32.
  AFTER ALL FIXES: all 5 endpoints + zero-rate PASS in 5-8s each, first 429 within
  30-75 requests (self-healing tagging works from txn #1).
  force_restart.inc was tried in the integration .test files then REVERTED (user: hack;
  also empirically unnecessary — MTR re-seeds write IDENTICAL fixture data so RDRS caches
  revalidate; the documented re-seed poisoning only bites when fixtures CHANGE between
  seeds). sharing test = internally racy pre-existing (3 runs → 3 different failure sets,
  incl. RonSQL-parity subtests rate-limiting doesn't touch; fails even with fresh servers;
  sharing matrix is STATIC fixture data → varying results = requests racing RDRS's async
  api-key/FS-metadata cache population after seed).
  OPERATIONAL TRAP (done TWICE): pkill of runtime_output_directory processes while an MTR
  run is still active kills the run's own cluster → bogus test failures (exit: 9 = my
  SIGKILL in the server line). ALWAYS confirm the background MTR task completed first.

- **SHARING FLAKE ROOT CAUSE (2026-07-29 late)**: api-key EVENT WATCHER prefetch races the
  seed — hopsworks fixture inserts api_key rows BEFORE share/team rows; key-INSERT event →
  load_single_key → get_user_databases reads shares mid-seed → entry cached VALID with
  PARTIAL grants → 401s ("not authorized to access <db>") until next refresh; default
  CacheRefreshIntervalMS=180000 (3 min!) > sharing package runtime → whole-package flake.
  Solo passes because the watcher's subscription dies with the prior hopsworks DROP and
  its 1-30s retry backoff misses the seed events. FIX (test env): CacheRefreshIntervalMS
  5000 in rdrs_config_template.json. PRODUCT BUG (user's design call, RONDB-1021 owner):
  new API key can be authorized-but-GRANTLESS for up to CacheRefreshIntervalMS in prod —
  event prefetch has no "grants complete" barrier. not_parallel marker for sharing was
  tried and REVERTED (user: hacky; also proven ineffective — failed in not_parallel phase).
  RARE ENVIRONMENTAL MODE (1× in 11 suite runs): hard NDB schema-dist stall — both mysqlds
  stop progressing DDL, go-test seed hangs, MTR 900s timeout kills test (index_scan run
  #11); same class as the suppressed "Schema dist coordinator detected timeout" warning;
  pre-existing NDB robustness area, unrelated to rate limiting.
  SOAK RESULTS: run #10 = 22/22 ALL GREEN (first ever); run #11 = 21/22 (the stall);
  sharing passed in normal parallel phase post-marker-removal (A/B proof for 5s refresh);
  run #12 = 21/22 (drop-gap invalid-cache race → FIXED by readiness probe:
  testutils.WaitForAPIKeyAuthReady polled from InitialiseTesting, replaces "need a more
  reliable way" 1s sleep); run #13 with probe = 22/22.
  COMMITTED 2026-07-30 on RONDB-978-port: 6eb79508565 (self-healing user-id resolution:
  Dbdict REP char-fix + ClusterMgr upsert/GET-fallback) + c0662269def (deterministic MTR
  suite: duration burst, pooled client, 5s refresh, readiness probe, $-exclusion,
  suppression). NOT committed by user instruction: 3× t.Skip unload_schema_test.go —
  USER stashed them themselves (stash@{0} "WIP on RONDB-978-port"); pop when running MTR
  locally until the RONDB-1092 port lands. Also committed: adce0003931 (unified Hopsworks
  copyright headers, 79 files, "Copyright (c) 2024, 2026, Hopsworks and/or its
  affiliates.", comment-style-preserving; .result/.cnf/.test/.inc/.json intentionally
  header-less). Branch = 15 commits ahead of upstream/26.02-main, unpushed.

## AGREED NEXT TASK (Mikael review comment 2026-07-30) — user-id cache: authoritative + periodic re-LIST
Mikael flagged: after 6eb79508565, every transaction for an unprovisioned user pays a
blocking GET_DATABASE_REQ (per-request DICT round trip — hits customers with
UserRateLimits on but no users provisioned). AGREED FIX (proposed as "option B", Mikael's
negative-cache idea subsumed): restore ORIGINAL authoritative-cache semantics now that
the REP push channel is fixed:
1. retrieveUserId: m_initialised && miss → return RNIL immediately (unattributed, NO probe)
2. GET_DATABASE_REQ fallback ONLY while !m_initialised (startup window)
3. NEW periodic re-LIST of user table (~60s; reuse fillingUserIdCache walk + reconcile
   pass so missed DROPs heal too) — backstop for a REP lost during client disconnect
Trade-off accepted: lost-REP worst case = new user unmetered up to ~60s; steady state zero
round trips. Test: Go test creating a user AFTER RDRS start, assert throttling engages via
push path alone (no per-txn probes). STATUS: agreed on Slack, NOT yet implemented.

- **TICK-RACE ROOT CAUSE (2026-07-29, MTR suite failures)** (superseded by FULL ROOT CAUSE above): kernel enforcement is
  FLAG-BASED: per-op TCKEYREQ code only checks m_is_queueing_abort[_read] flags; flags are
  recomputed from accounted usage on 100ms ticks (execDATABASE_RATE_ORD, DEB_RATE macros are
  COMPILED IN on this branch → ndbd.log at var/mysql_cluster.1/ndbd.N/ndbd.log shows
  "(inst):dbId overload: X, queueing_time: Y" every 100ms — invaluable). A 400-req burst at
  15-parallel on a WARM cluster completes in ~60-120ms < one tick → 0 throttled → test fails;
  cold/loaded systems span ticks → passes. Failing runs took 5.06s (2×2.5s settle + fast burst),
  passing 7.6s — duration encodes the race. FIX (testutils ratelimits.go, uncommitted): priming
  wave of 30 reqs + 500ms settle after setting rate=1 → at rate 1µs/s one op latches the flags →
  deterministic. Alter distribution verified CORRECT (all TC+LQH instances, both nodes, in logs).
  ALSO: drop user → 709 AND poisons subsequent set/get/drop user ops (fresh names!) on that
  cluster — real kernel bug, admin-only, still open. mtr_check.sql (GLOBAL file) got
  `root_name NOT LIKE '$%'` exclusion in check_testcase_ndb() (uncommitted) because leftover
  $-users otherwise fail EVERY test's check-testcase state diff (+1355 $bkYjEz...).

- **TEST LAYOUT (reorganized 2026-07-28)**: the standalone `internal/integrationtests/ratelimit`
  package was DELETED (+ its MTR .test/.result + run_gotest.inc map line — run_gotest.inc has a
  STRICT check that .test files == map keys AND `go list ./...` == map values, so all three must
  move together). Each endpoint's rate-limit test now lives in its OWN package as `ratelimit_test.go`
  and reuses that package's native request builders (no duplicated body/URL builders):
  pkread (TestRateLimit + TestRateLimitZeroIsUnlimited, via NewFiltersKVs/NewPKReadURL, int_table1),
  batchpkread (inline api.BatchSubOp), feature_store (CreateFeatureStoreRequest + fsNameSimple/
  fvNameSimple consts = fsdb001/sample_1/id1), batchfeaturestore (CreateBatchFeatureStoreRequest,
  FSDB001/sample_1), index_scan (api.IndexScanQuery + NewIndexScanURL, db029/tiny_tbl/val_1 GE 1).
  DropRateLimitUser removed (was dead code). Sender uses SendHttpRequestWithClient (shared client).
  .result files are per-package `ok <pkg>` summaries (duration stripped, no -v) so adding test fns
  needs NO .result edits. VERIFIED GREEN 2026-07-28: all 5 endpoints throttle→429+recover, zero-rate
  unlimited passes, sibling FS test passes alongside (ordering: ratelimit_test.go sorts last → runs
  after normal tests; defer restore+settle keeps cluster unthrottled). go vet clean whole module.

## LATENT RONDB-978 FEATURE BUGS FOUND (all exist upstream 26.04-main too; feature
## was never end-to-end tested — MTR ndb_rate_limits accepts zero errors!)
1. **mgmd SIGSEGV on `set user`**: Services.cpp setUser/alterUser/dropUser/getUser parsed
   username then passed nullptr database_name to *_quotas → strnlen(NULL). FIXED (pass
   user_name). Verified set/alter/drop return Ok now.
2. **is_user flag lost in schema ops**: MgmtSrvr set/alter/drop_quotas put is_user in
   req->requestInfo, but Dbdict startClientReq NEVER copies requestInfo into m_request
   (only requestType; DictSignal requestType = bits 0-7 collision anyway) → create/drop/
   alterDatabase_parse read stale/zero → `set user` created a PLAIN DATABASE named like
   the user (no $) — get user/list users empty while DATABASE QUOTA GET finds it.
   FIXED: is_user now travels as packed property db_obj.IsUser (already in mapping,
   already packed by packDatabaseIntoPages for restarts); parse fns read db.IsUser after
   unpack (3 sites), mgm sets db_obj.IsUser (3 sites).
3. **ClusterMgr user-id cache client bugs**: (a) fillingUserIdCache used safe_sendSignal
   from app thread → assert is_poll_owner_thread → rdrs2 SIGABRT on first tagged request;
   (b) missing tSignal.setSignal(GSN_LIST_DATABASE_REQ) (malformed signal); (c)
   retrieveUserId held theUserIdMutex and called insertUserId which locks it again
   (self-deadlock); (d) mutex leak on miss-path returns. FIXED: setSignal added;
   new startUserIdCacheFill() (lock/raw_sendSignal/flush/unlock, called after releasing
   theUserIdMutex); miss path returns 1 → per-name GET_DATABASE_REQ resolves (thread-safe
   sendRecSignal); no more RNIL placeholder insert from retrieveUserId.
4. **`get user` returns "No such user exists" even after successful set** — was because
   of bug 2 (entity was a database); re-verify after fix.
5. **Rate enforcement "inert" ROOT CAUSE = config, not code**: ALL LQH usage
   accounting is gated by `m_rate_limits_active` from cluster config param
   **`ActivateRateLimits`** (`[cluster_config.ndbd.*]`, CFG_DB_ACTIVATE_RATE_LIMITS=687,
   default false). suite/ndb_quota/my.cnf sets ActivateRateLimits=1 per ndbd; the
   rdrs2-golang suite didn't → nothing metered. FIXED: added to
   mysql-test/suite/rdrs2-golang/my.cnf for both ndbd sections.
6. **GET_DATABASE_CONF/REF dispatch dead**: Ndbif.cpp derived the transaction ptr
   from signal word 0, but DICT puts its own ref there and the client cookie in
   clientData (word 1) → every setUserId GET timed out (~2min WAITFOR). FIXED:
   Ndbif reads word 1 (aSignal->readData(2)) for these two GSNs.
7. **insertUserId ended with NdbMutex_Lock instead of Unlock** (ClusterMgr.cpp) →
   errorcheck-mutex assert on first LIST fill insert → rdrs2 abort. FIXED.
8. LATENT (not yet fixed): Dblqh::execLQHKEYREQ var_index only incremented when the
   user hash lookup FINDS the record — on a miss with the flag set, applRef/applOprec
   read shifted words (userId read as applRef). Also `get user` mgm command still
   returns "No such user exists" for existing users (get_quotas is_user lookup bug).
9. TC forwards userId to LQH only via m_queuedDatabasePtrI association
   (check_tckey_queueing sets it on first touch — OK) — reviewed, works once (5)
   is on; scan path associates in execSCAN_TABREQ (line ~16012).

## ✅ WORKING END-TO-END (2026-07-27 night): 364/400 reads → HTTP 429 at rate=1

Two more root-cause bugs found & fixed to make USER enforcement actually work:
- **7th bug — NdbRecord path never set the userId flag.** setUserId resolved the id on
  the transaction, but `NdbOperation::fillTcKeyReqHdr` (NdbOperationExec.cpp, the
  NdbRecord/RDRS path) had NO userId handling — only the legacy `prepareSend` did. So
  every RDRS TcKeyReq went out with user_id_flag=0. FIX: fillTcKeyReqHdr now writes
  userId/version into the first optional slot(s) and sets the flag when
  theNdbCon->m_user_id != RNIL (mirrors prepareSend; scan path already did this in
  NdbScanOperation.cpp:2785).
- **8th bug — ClusterMgr user-cache hash units.** `rondb_calc_hash_val(key, len, use_new)`
  takes len in 4-byte WORDS (keylen_bytes = 4*len) but ClusterMgr passed the byte length
  → hashed 4× the bytes, reading OOB garbage → SAME username hashed to DIFFERENT buckets
  on insert (heap entry) vs lookup (request buffer) → cache always missed → after LIST
  fill marked cache "initialised", every lookup returned RNIL (no such user). FIX: new
  `ClusterMgr::calc_user_hash_index()` copies the name into a zero-padded word-aligned
  buffer and passes the word count; used at all 5 hash sites (retrieve/insert/update/
  delete/rateOverflow). Verified: insert & lookup now agree (inx=372), bucket found,
  m_user_id=13 resolves, TC user_id_flag=1, LQH find=1, accumulation → throttle.

Proof: user rate=1, blast 400 reads (15-way parallel) → 36×200 + 364×429.

KNOWN FOLLOW-UP (not blocking): RDRS connection pool = multiple Ndb connections, each
its own ClusterMgr user cache; some connections showed user_id_flag=0 (cold/unresolved)
→ uneven accounting across the pool. Throttling still fires strongly; revisit if precise
accounting per pool matters.

REMAINING WORK: strip all RLDBG/RLDBG2/RLDBG3 debug logging (NdbTransaction.cpp done;
NdbOperationExec.cpp, DbtcMain.cpp, DblqhMain.cpp, ClusterMgr.cpp still have it), clean
rebuild (rdrs2 + ndbmtd + ndb_mgmd), re-run Go ratelimit + full suite.

## Per-endpoint coverage + 2 MORE bugs (2026-07-27 night, continued)

User expanded test to cover ALL endpoints except ronsql: pk-read, batch-pk-read,
feature-store, batch-feature-store, scan. Rewrote ratelimit_test.go table-driven
(TestRateLimitPerEndpoint subtests). Found 2 more bugs:

- **9th bug — scan buddy transaction.** NdbScanOperation::init (NdbScanOperation.cpp:153)
  runs the scan on a hupp'd buddy transaction (theNdb->hupp(myConnection)), NOT the
  caller's txn. setUserId set m_user_id on the caller txn; the ScanTabReq read
  theNdbCon(buddy)->m_user_id = RNIL → scans never tagged. FIX: after theNdbCon =
  aScanConnection, copy m_user_id/m_user_id_version from myConnection to the buddy.
- **10th bug — DBLQH var_index conditional (CRASH).** execLQHKEYREQ (DblqhMain.cpp:9064)
  did `var_index++` INSIDE `if (found)` of the user-hash lookup. The userId word is
  present whenever the flag is set, so on a block that lacks the record — notably
  QUERY threads (DBQLQH), which don't hold user DatabaseRecords — var_index stayed 0,
  the fixed-length check (9366 FixedSignalLength+nextPos != siglen) failed →
  LQHKEY_error(2) → ndbabort → BOTH data nodes crashed (error 2301) under batch-read
  load. FIX: var_index++ unconditional on the flag; only m_user_ptr_i assignment
  depends on find. Sender/forward site (12649) was already consistent (writes word
  only when setting flag) — no change.

- **11th bug — scan var_index twin of #10.** execSCAN_FRAGREQ (DblqhMain.cpp:19977) had
  the SAME conditional `extra_len_index++` inside `if (find)`. After #10 was fixed, the
  SCAN burst still crashed both nodes (error 4028/146 node failure). FIX: same —
  extra_len_index++ unconditional on ScanFragReq::getUserIdFlag. Verified other sites
  are fine: DBLQH 5403/6963/18270 use `var_index = getUserIdFlag` (0/1, sender-side,
  correct); DBTC receivers read userId at a fixed union offset (no var_index desync);
  DBSPJ increments unconditionally. Only the two DBLQH receivers (9065 key, 19977 scan)
  were buggy; both fixed.

- **12th bug (SECONDARY, admin-only, NOT fixed) — mgm `drop user`/`get user` 709.**
  `list users` shows the entity as `$bkYjEz6OTZyevbqt` (unstripped) and drop/get return
  709 "No such user". The client-side GET_DATABASE_REQ path resolves fine (enforcement
  works); only the mgm drop/get admin commands mis-resolve the $-prefix. Does NOT affect
  rate limiting. Test worked around it: TestZeroRateIsNotLimited uses rate_per_sec=0
  ("no limit" per DbtcMain:26864) instead of drop to assert the unlimited path.

TEST RESULT after #9/#10 fixes (before #11): all 5 endpoints throttled BUT scan burst
crashed nodes again (#11). After #11 fix: re-verification in progress (this is the
expected green run — all 5 endpoints throttle, no crash, TestZeroRate unlimited).

- **13th bug — getAllowStartTransaction null deref on user records (CRASH).**
  Dbtc::getAllowStartTransaction (DbtcMain.cpp:27639) does
  `databaseRecordPtr.p->globalDatabaseInstance->startNewTransactionAllowed()` for
  !is_committed_read (writes / locking reads). globalDatabaseInstance is set ONLY via
  CONNECT_TABLE_DB_REQ which is database-only (ndbrequire m_is_user==false, 27397), so
  USER records keep it nullptr → null deref → SIGABRT (error 6000) when a write/locking
  read transaction is tagged with a user. Surfaced in the regression run (pkread/
  index_scan packages, which do non-committed-read ops) once rate-limiting-by-default
  routed ALL test txns through the user path. FIX: skip the parallel-transaction quota
  for user records — `if (!is_committed_read && !databaseRecordPtr.p->m_is_user)` —
  correct per design (users have rate limits only, not max_parallel_transactions).
  One fix in getAllowStartTransaction covers all its call sites.

Running tally: 13 bugs in the ported RONDB-978 feature; 12 fixed (all load-bearing),
#12 (mgm drop/get user $-resolution) admin-only, worked around in tests.
The dedicated ratelimit package went FULLY GREEN (all 5 endpoints + zero-rate) before
#13 was found; #13 surfaced only in the broader regression (write ops). Re-verify both
after the #13 rebuild.

## ✅ FINAL (2026-07-27 night): stage-1 API-key rate limiting COMPLETE & verified

ratelimit package GREEN (twice, incl. after #13): TestRateLimitPerEndpoint subtests
pk-read / batch-pk-read / feature-store / batch-feature-store / scan all throttle with
HTTP 429 at rate=1; TestZeroRateIsNotLimited PASS. Nodes stay up under load (all
crash bugs fixed). 13 bugs fixed total (see above); #12 (mgm drop/get user $-resolve)
admin-only, worked around.

REGRESSION CAVEATS (NOT rate-limit regressions — do NOT chase as such):
- TestUnloadSchema (pkread/batchpkread/index_scan) crashes RDRS via a pre-existing
  dictionary-lifetime UAF: SIGSEGV in NdbDictionary::Column::getColumnNo() ->
  set_operation_pk_col -> setup_primary_keys during concurrent schema unload. This is
  the RONDB-1092 area ([[rdrs-schema-race-crashes]]); my rate-limit code is NOT in the
  stack. Crashes with or without rate limiting.
- batchfeaturestore "feature view was deleted" = cross-package test-data isolation
  (feature_store cache_event tests delete FVs); MTR per-package isolation avoids it.
- Running many Go packages back-to-back manually also hits db025 leftover + api-key
  cache poisoning after re-seed (restart RDRS after any re-seed).

OPEN CONSIDERATION for user: rate-limiting is enabled by DEFAULT for the whole
rdrs2-golang suite (my.cnf ActivateRateLimits=1 + config UserRateLimits=true + suite
provisions high limit). This routes every test txn through the user-quota kernel paths.
It's now crash-safe (13 fixes), but if a reviewer prefers, it could be scoped to just
the ratelimit test instead of suite-wide. TestUnloadSchema's pre-existing UAF is
orthogonal.

Change set (git diff --stat): 24 tracked files + new rate_limit.hpp + Go test files
(mgmclient, ratelimits.go, ratelimit package, MTR .test/.result/map). All debug
instrumentation removed (grep RLDBG = 0).

## END-OF-SESSION STATUS (2026-07-27 late)
STAGE-1 RDRS WORK = DONE & PROVEN CORRECT. Instrumented setUserId confirmed the
client resolves the identity end-to-end:
  RLDBG setUserId('bkYjEz6OTZyevbqt') cache ret=1 user_id=RNIL   (miss, sends GET)
  RLDBG GET_DATABASE_CONF user_id=1355 ver=1                     (resolved, tagged)
429 mapping PROVEN: a DATABASE quota (ndb_mgm "DATABASE QUOTA SET db004 --rate-per-sec=2")
threw 44/100 HTTP 429 on the REST pk-read path — enforcement machinery + my 429
translation both work.

**REMAINING BLOCKER (7th bug, DEEP, pre-existing, NOT my RDRS code): USER-entity
enforcement doesn't throttle** even though the txn is correctly tagged (userId 1355)
and there is NO "incorrect user id" version-mismatch rejection in the data nodes.
DATABASE-entity enforcement works; USER-entity path does not. Suspects not yet
confirmed (each needs a ~7min kernel rebuild to test):
  - DBTC execTCKEYREQ gate `if (user_id_flag && handle.m_cnt != 0)` (DbtcMain.cpp:3699)
    — ruled LESS likely (RDRS long signals carry sections).
  - LQH execLQHKEYREQ only sets m_user_ptr_i when the user-id hash lookup succeeds;
    var_index bug (DblqhMain.cpp:9064-9075) — on a MISS with flag set, applRef/applOprec
    read shifted words. If the LQH lookup by database_id misses (id space mismatch
    between what client sends = DICT databaseId 1355 vs what LQH keyed the record on),
    m_user_ptr_i stays RNIL → accumulation falls back to table→db (db004, no quota) →
    no throttle. **LEADING hypothesis: verify the id the client sends (DICT databaseId)
    equals the key LQH/TC used in m_databaseRecordHash for the user record.**
  - version field: GET_DATABASE_CONF returns ver=1 but `list users` prints
    userVersion=1355 (likely a list display bug, but confirm TC's record version==1).

Debug logging was REMOVED from NdbTransaction.cpp; rebuilt clean. No debug code left.

**SCOPE QUESTION for user**: stage-1 grew into debugging the entire never-functional
RONDB-978 kernel feature (6 crash/correctness bugs fixed today + this 7th enforcement
bug). The 7th is in shared DBTC/DBLQH rate accounting (also affects the MySQL path,
not just RDRS). Decide: (a) continue into kernel debugging of USER enforcement, or
(b) land the RDRS stage-1 + the 6 kernel fixes as-is and file the USER-enforcement
bug separately. All 6 fixes are needed regardless (mgmd crash etc.).

## Test cluster state
rdrs2-golang cluster up w/ ActivateRateLimits=1 (my.cnf fix). Test key
bkYjEz6OTZyevbqt user entity exists (id 1355), currently rate 2 or restored-high.
Go ratelimit tests: TestUnprovisioned PASSES; enforce/batch tests correctly FAIL
(they're right — enforcement is genuinely broken for users). ndb_rate_limits MTR is
a false-green (accepts 0 errors). Retuned test constants: rate unit = us-work/sec,
reject at overload(=used/rate)>=30 read / >=60 all; recovery only by RAISING limit.

## Cluster/test env state (2026-07-27 evening)
- Fresh MTR cluster (var regenerated, config includes UserRateLimits=true): mgmd 13000
  (manually restarted w/ fixed binary: `../runtime_output_directory/ndb_mgmd
  --defaults-file=$PWD/var/my.cnf --defaults-group-suffix=.1.1 --mycnf --nodaemon
  --configdir=$PWD/var/mysql_cluster.1/ndb_mgmd.1` from build/mysql-test, ABSOLUTE
  configdir required), 2 ndbmtd, mysqld 13001/13003, rdrs 13005 (noTLS)/13006 (TLS)
  restarted manually from runtime_output_directory with var configs.
- DBs seeded (sentinel exists). Test cruft: test1 db (t1,t2) created for enforcement
  probes; quota records may linger — clean up before final test runs.
- Go tests: TestUnprovisionedIdentityIsNotLimited PASSES; enforce tests fail (bug 5).
- MGM protocol notes for Go client: BSD nc unreliable — use python/net.Dial;
  "list users\nnextUserId: 0\n\n"; num_rows field in list reply.


## 26.02 branch RONDB-978-Cont (2026-07-31, VALIDATED+PUSHED 2026-08-10)
Working in <repos>/rondb (NOT rondb2). Rebased 2026-08-10 onto
upstream/26.02-main tip ea4df44859f (across the MySQL 8.4.11 upmerge, ~250 commits).
PUSHED to origin (smkniazi fork, force-with-lease) = 6 commits:
5aaf5b9bbb2 atomic seeder · 40dee85245d user-id cache authoritative + 60s re-LIST ·
f291524b4fd mgm USER family + drop-709 fix · 5b32fa7783e DBLQH scan UAF fix ·
124ec9a8d4d push-path Go test · 91f87711bc7 suite cleanup USER DROP.
Cache change: retrieveUserId returns unmetered RNIL (no per-txn GET_DATABASE_REQ) once
m_initialised_user_id_cache; GET fallback only during warmup; threadMain re-LISTs user
table every 60s (USER_ID_RELIST_INTERVAL_MS), upsert+generation-stamp in insertUserId,
sweepStaleUserIds() removes missed-DROP entries.
**MTR VALIDATED 2026-08-10: ndb_quota 13/13; rdrs2-golang ALL GREEN (both phases,
0 node crashes, check-testcase clean; suite ~11.5min).**
**PR to logicalclocks/rondb 26.02-main: BLOCKED on `gh auth login` (not authenticated
on this box). PR body saved at rondb/.claude/RONDB-978-Cont-pr-body.md — create with:
gh pr create --repo logicalclocks/rondb --base 26.02-main --head smkniazi:RONDB-978-Cont
--title "RONDB-978: User rate limit follow-ups: authoritative user-id cache, mgm USER
command fixes, drop-user UAF fix" --body-file .claude/RONDB-978-Cont-pr-body.md**

## NEW KERNEL BUG FOUND+FIXED 2026-08-10: DBLQH scan m_user_ptr_i UAF (5b32fa7783e)
The push-path test (first workload ever to USER DROP on a live-traffic cluster)
crashed both ndbmtds: error 2301 "Invalid memory access" DBLQH, magic=0 on released
RWPool64 page. ROOT CAUSE: seizeTcrec recycles TcConnectionrec from LQH's own free
list WITHOUT re-construction (TransientPool::seize's placement-new only runs for
list-refills), and initScanrec only ASSIGNED m_user_ptr_i on user-hash hit — never
cleared it. Untagged scan (e.g. RDRS api-key refresh) inherits stale pool index from
a previous tagged op → after USER DROP releases the DatabaseRecord (page freed when
empty) → update_rate_usage in execTUPKEYCONF/REF derefs → abort. The bool-returning
RecordPool64::getPtr guard is DEAD CODE (delegates to aborting RWPool64::getPtr).
FIX: unconditional m_user_ptr_i=RNIL64 in initScanrec (mirror of execLQHKEYREQ:9115).
In-flight tagged ops are safe by DESIGN: DICT drop = TC PREPARE_DROP (waits
m_api_ref_count==0, scans included) → LQH COMMIT_DROP → TC COMMIT_DROP.
**Same bug exists on 26.04-main/26.05-main — needs the mirror fix there (told
reviewers in PR body).**
Also fixed: rdrs2-golang suite leaked the provisioned $-user → check-testcase fail
(first suite run since the mtr_check $-hack removal); cleanup USER DROP in
run_gotest.inc, wrapped in --disable_result_log (mgm stdout varies with existence).
Debugging trap hit twice: 900s MTR seed timeouts on a loaded/hot machine looked like
the schema-dist stall but were just slowness — --testcase-timeout=40 cured; suite
back to ~11.5min once the box idled.

## CROSS-CHECK vs HOPSWORKS 2026-08-11: 4 MORE issues found+fixed, commits REWRITTEN
Final commits (replaced 580805c6ec3/38c32900d43/6e351b53909): **aab9edbfbaf** (impl) +
**7c814f91450** (tests) + amended doc commit on top. Issues rooted out after user pushback:
(1) `_featurestore` map alias was DEAD SPECULATION — feature_store.name = BARE lowercased
project name (FeaturestoreController.java:239); alias removed. (2) CLIP BOUNDARY: Hopsworks
clips only length>32 → substring(0,31); a 32-char name SURVIVES (23-char project). Was
clipping >31 → bucket mismatch. MAX_LEN now 32, resize(31) under >32. (3) RESTRICTED
members: setupOnlineFeatureStore creates the MySQL account for EVERY project_team row
(role only selects grants) → restricted memberships now carry identities too. (4) REVIEWER
HIGH: NdbTransaction::init() never reset m_user_id/m_current_username → recycled pooled
txn made "unmetered" requests inherit the PREVIOUS request's identity (cross-bucket 429s)
+ UAF read of freed identity string on rate-overflow path; fixed in init() (also cures
pre-existing RonSQL leak). Also: config PROBLEM username-mode-requires-UseHopsworksAPIKeys
must live in the AllConfigs PROBLEM block (validate() runs on the CANDIDATE before
globalConfigs assignment — referencing globalConfigs there reads the OLD config).
VERIFIED: ha_ndbcluster.cc:7756 tags with bare security_context()->user() (no @host) →
REST/MySQL buckets coincide. Suite green post-fixes (627s+758s runs). LESSON: mirror the
exact Hopsworks lines, never paraphrase.

## USERNAME IDENTITY MODE IMPLEMENTED 2026-08-10 (stage 2; commit hashes superseded — see above)
RateLimitIdentity="username": identity = clip31(ProjectName + "_" + users.username) —
EXACTLY Hopsworks' online-FS MySQL account (OnlineFeaturestoreController.onlineDbUsername,
incl. the substring(0,31) off-by-one; sharing grants attach to these accounts). api_key
cache reads users.username + precomputes per-MEMBER-project identity map keyed by online
db AND "_featurestore" db; authenticate() overloads gained optional RateLimitIdentities*
out-param (filled under entry lock); rate_limit.hpp does map lookup; unknown db (shared
stores!) = unmetered. Billing: pk-read/scan=URL db, batch=first op's db (grouping=followup),
FS/batch-FS=the FV's OWN project (shared FGs billed to entry project), ronsql=TAGGED for
the first time (RonSQLExecParams.rate_limit_identity → setUserId at RonSQLPreparer.cpp
startTransaction; 429 mapping = follow-up, throttle surfaces as temporary-500). MTR: server
rdrs.2.1 runs username mode (own template rdrs_config_template_username.json), Go harness
mode-aware (rateLimitIdentity(billingDb), HOPSWORKS_TEST_USERNAME="macho"); fixture: macho
now MEMBER of dynamic db-projects + fsdb001 (shares kept); cleanup drops db004_macho/
db029_macho/fsdb001_macho too. Suite green BOTH modes (gotest 1142s, 0 skips, 0 crashes).
Design doc: docs/RONDB-978_username_rate_limit_design.html + artifact
https://claude.ai/code/artifact/e9e14c65-210c-47af-a27c-e4b1500c11a0. Branch = upstream+9,
3 new commits UNPUSHED (origin has first 6). Hopsworks-side provisioning NOT implemented
(operation_log handler / co-located with createOnlineFeaturestoreUser — see doc §7).

## RESOLVED 2026-07-31: `USER *` mgm command family + drop-user 709 FIXED; $-hack removed
The whole "USER SET/GET/LIST/ALTER/DROP" mgm command family was non-functional. The
"drop-user 709" was NOT a DICT name-lookup bug (create/drop/get are byte-symmetric —
that's why it never byte-root-caused). It was a stack of 5 independent defects, found
empirically via MTR repro `ndb_quota/ndb_user_lifecycle.test` + kernel g_eventLogger
+ JAM trace (see below). All fixed on branch RONDB-978-Cont, test passes, then the
`root_name NOT LIKE '$%'` skip was removed from mtr_check.sql.
  1. `mgmclient/CommandInterpreter.cpp` — `executeUser*` were CLONED from
     `executeDatabaseQuota*`, which strip a leading GROUP word ("QUOTA") via
     `strchr(parameters,' ')`. "USER <sub>" has NO group word (USER is the dispatch
     firstToken), so the strip ate the SUBCOMMAND token → command_list[0]=username →
     "Command inconsistency" (SET/ALTER/DROP) or OOB index (GET/BACKUP/RESTORE/LIST).
     Fix: split `parameters` directly (no strip) in all 5 handlers + adjust size guards.
  2. `mgmsrv/MgmtSrvr.cpp get_quotas` — declared `num_rows: 9` but the is_user branch
     emits only 6 rows → client (ndb_mgm_get_user) blocks reading the missing 3 →
     "Timeout reading Get user". Fix: emit `num_rows: 6` for the user branch.
  3. `mgmsrv/MgmtSrvr.cpp list_quotas` — declared `num_rows: 10` but user branch emits
     8 → same hang. Fix: `num_rows: 8` for user branch. (backup_quotas was correct: 1 row.)
  4. `mgmsrv/Services.cpp` — server registered `"list users"`/`"backup users"` (plural)
     but client sends singular `"list user"`/`"backup user"` → "Unknown command". Fix:
     rename registrations to singular + backupUsers reply "backup user reply".
  5. `dbdict/Dbdict.cpp` (THE REAL 709) — createDatabase_parse/alterDatabase_parse
     prepend `$` unconditionally for is_user. Master converts "alice"→"$alice", stores
     it, then packDatabaseIntoPages RE-PACKS the converted "$alice" into the op section
     for participants + schema file. Participant re-parses "$alice" and prepends AGAIN →
     stores "$$alice". DROP runs parse on ALL nodes: master has "$alice" (found), the
     participant has "$$alice" (misses "$alice") → 709 aborts the trans. GET/LIST hit
     only one node so masked it. Fix: idempotent guard `db.DatabaseName[0] != '$'` in
     BOTH create and alter parse. (drop/get parse are fine — name arrives unprefixed
     from client, not repacked.)
  6. `dbdict/Dbdict.cpp` (exposed by fixing #5) — dropDatabase_commit armed
     `dropDbPtr.p->m_callback = dropDb_alterComplete` ONLY on the `!is_user` branch; the
     is_user branch sent DROP_DB_REQ to DBTC without arming it. Both paths finish via
     dropDb_completeBlock → `execute(signal, m_callback, ...)` → null callback →
     `ndbrequire(fun!=0)` node abort (SimulatedBlock.hpp:2438). Fix: hoist the callback
     assignment above the if so both branches arm it.
Debugging tools: JAM/trace files have NO repo .md; format is self-documented by a
legend appended at end of each `var/.../ndbd.N/ndb_N_trace.log.1*` (ErrorReporter.cpp
:485). Newest-signal-first; the `SOURCE FILE / LINE` pairs under each signal are the
jam() breadcrumb trail → mapped `Dbdict.cpp:34342→SimulatedBlock.hpp:2438` to the crash.
New regression test: `mysql-test/suite/ndb_quota/ndb_user_lifecycle.test` (SET→GET→
LIST→ALTER→GET→DROP→GET-must-fail; result "USER lifecycle completed").
