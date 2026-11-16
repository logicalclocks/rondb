# RONDB-978: Username-based rate limiting — status & why it was held back

_Compiled 2026-09-23 from git history, the design doc on `origin/RONDB-978-Cont`, and
Claude session memory. Session transcripts from Jul/Aug 2026 are pruned; GitHub PR
discussion was not accessible when this was written._

## TL;DR

Username-identity rate limiting **was fully implemented and tested** (both identity
modes green in MTR), but was **deliberately held back from upstream**. On 2026-08-18
the branch was split: a filtered branch `RONDB-978-API-KEY-Cont` (all username commits
stripped) merged into **26.02-main** via the 25.10→26.02 upmerge. The username work
sits **unmerged** on `origin/RONDB-978-Cont` (smkniazi fork).

## What merged to 26.02-main (verified 2026-09-23, rebadged hashes)

| Commit | Subject |
|---|---|
| `1651eacc4f6` | Move rate limit settings to their own .RateLimit config section |
| `4880413b1d4` | Answer 429, not 500, when RonSQL is rate limited |
| `a99bc88b752` | Tag RonSQL transactions with the rate limit identity |
| `563ae360e8d` / `931ad525173` | Clear rate-limit user id / pooled state on transaction & scan reuse |
| `0bbd8da24b7` | Test that USER rate limits throttle SQL traffic |
| `133e5d67403` | Fix the mgm USER command family and drop-user 709 |
| `7a5c4f9559d` | Make the user-id cache authoritative with periodic re-LIST |
| `e447125b7f4` | Clear stale user id on scan TcConnectionrec reuse (DBLQH UAF) |

## What did NOT merge (only on origin/RONDB-978-Cont, tip 99eb63a9a0c)

- `aab9edbfbaf` — **Support Hopsworks project-user names as rate limit identity**:
  identity = `clip31(ProjectName + "_" + users.username)`, byte-for-byte equal to
  Hopsworks' online-FS MySQL account (`OnlineFeaturestoreController.onlineDbUsername`,
  including its `substring(0,31)` off-by-one), so REST and MySQL traffic of one project
  member share a kernel bucket (`ha_ndbcluster::set_user_id` tags SQL sessions with the
  bare account name — the strings must match exactly).
- `7c814f91450` — dual-mode MTR suite (server 2 = username mode via
  `rdrs_config_template_username.json`, `HOPSWORKS_TEST_USERNAME=macho`).
- `8a178f79aee` / `57e3ff25f62` — design doc + corrections:
  `storage/ndb/rest-server2/server/docs/RONDB-978_username_rate_limit_design.html`
  (also as artifact: https://claude.ai/code/artifact/e9e14c65-210c-47af-a27c-e4b1500c11a0)
- `1bc7a4846ca` — Bill shared feature store reads to the recipient project.

Test status when parked: ndb_quota 13/13, rdrs2-golang green in BOTH identity modes
(0 skips, 0 crashes).

## Why username mode was held back (design doc §7 / §9 / §10)

1. **Inert without Hopsworks-side provisioning (§7).** The kernel only meters
   identities that exist as `$`-prefixed USER entities (mgm `USER SET`); unprovisioned
   identities run unmetered, so flipping the mode without provisioning is a no-op.
   Provisioning belongs in **hops/hopsworks** (cross-repo): `USER SET`/`USER DROP`
   co-located with the MySQL account lifecycle in `OnlineFeaturestoreFacade`
   (`createOnlineFeaturestoreUser` / `removeOnlineFeaturestoreUser`), or an async
   `operation_log` handler (`ONLINE_FS_DB_CREATE_USER`/`REMOVE_USER` rows already carry
   `online_fs_user` — the identity string; template: `SupersetUserAccountHandler`).
   Plus: service-user exemption (`operation_log.is_service_user`), SET-then-ALTER
   idempotency, and a periodic reconciliation job against `USER LIST`.
2. **Open product decisions (§10).** How to bill direct pk-reads/scans against a shared
   project's database the caller is not a member of (currently: unknown db = unmetered);
   whether mixed-database batches should split into per-project transaction groups
   (first-op-wins shipped as a stopgap); how JWT requests map to an identity.
3. **Hard dependency on Hopsworks API keys.** Config validation rejects
   `RateLimitIdentity="username"` unless `UseHopsworksAPIKeys` is on — the username half
   comes from `hopsworks.users.username` via the api-key cache. Plain RonDB deployments
   cannot use the mode.
4. **Rollout plan said ship dormant (§9).** Ship with `apikey` as default → ship
   Hopsworks provisioning → flip per cluster with a rolling restart. Splitting the
   branch let the uncontroversial fixes merge without waiting on review of the larger
   username change (touches `authenticate()` return types + api-key cache; the doc also
   post-mortems the broken original rondb2 `{project}_{user}` concatenation the port
   removed).

## Resuming this work

- RonDB side is done: code + tests already on `origin/RONDB-978-Cont` (needs rebase
  onto current 26.02-main; the apikey subset is already there with different hashes).
- Remaining work is mostly **Hopsworks provisioning** (doc §7, options A/B above) and
  the §10 product decisions.
- Constraint to preserve: the identity string must remain byte-identical to the online
  MySQL account name, or REST and SQL traffic stop sharing a kernel bucket.

## Related Claude memory files (persist across sessions)

- `~/.claude/projects/-Users-salman-code-hops-rondb/memory/rondb-978-stage1-rdrs-apikey.md`
- `~/.claude/projects/-Users-salman-code-hops-rondb/memory/RONDB-978-port.md`
