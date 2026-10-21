## Summary

Follow-up work on RONDB-978 user rate limits (base feature merged in #1015), rebased onto current 26.02-main tip. Six commits:

1. **Seed each test database in one transaction** — the Go test seeder now wraps each database's data load in a single transaction, closing the partial-read window that let the RDRS FS-metadata event watcher cache incomplete feature views mid-seed (deterministic sharing-test failures on fresh clusters).
2. **Make the user-id cache authoritative with periodic re-LIST** — per Mikael's review: a cache miss after initialisation now returns unattributed immediately instead of paying a blocking per-transaction GET_DATABASE_REQ round trip. CREATE/DROP_DATABASE_REP push announcements keep the cache current; a 60s re-LIST of the user table (with a generation-stamped sweep for missed drops) backstops a lost announcement. Accepted trade-off: a lost announcement leaves a new user unmetered for up to ~60s.
3. **Fix the mgm USER command family and drop-user 709** — five stacked defects made USER SET/GET/LIST/ALTER/DROP unusable (client parser token strip, get/list num_rows mismatches hanging the client, plural command registrations, kernel double-$-prefix on participants causing error 709, null m_callback aborting the node on user drop). Regression test: `ndb_quota.ndb_user_lifecycle`. Also removes the `$%` exclusion hack from mtr_check.sql.
4. **Clear stale user id on scan TcConnectionrec reuse** — LQH recycles TcConnectionrec from its free list without re-construction and initScanrec never reset `m_user_ptr_i`, so after USER DROP released the record, a later untagged scan dereferenced the dangling pool index in rate accounting and killed the node (error 2301, DBLQH). One-line fix mirroring execLQHKEYREQ. **The same bug exists on 26.04-main / 26.05-main.** In-flight operations were already safe via DBTC's PREPARE_DROP api_ref_count gate.
5. **Test that a user created after server start is enforced** — the agreed push-path validation: drop the provisioned user, verify unmetered, re-create with rate 1 (new DICT entity, new user id), require a 429 well inside the 60s re-LIST interval so only the CREATE_DATABASE_REP push can explain enforcement. This test is what exposed the UAF in commit 4.
6. **Drop the provisioned rate limit user in suite cleanup** — with the mtr_check hack gone, the rdrs2-golang suite must clean up the $-entity it provisions.

## Testing

- `ndb_quota` suite: 13/13 pass (incl. new `ndb_user_lifecycle`, `ndb_rate_limits`) on the rebased base.
- `rdrs2-golang` suite: all green (both non-TLS and TLS phases, all Go packages incl. the three per-endpoint rate limit tests and the new push-path test), zero data node crashes, check-testcase clean.

## Notes for reviewers

- **Per-connection accounting caveat (known, non-blocking):** each pooled RDRS Ndb connection has its own ClusterMgr user-id cache; a cold connection can send untagged operations, so accounting is uneven across the pool until all connections resolve the identity. Throttling still engages strongly.
- **Scope question:** rate limiting is enabled by default for the whole rdrs2-golang suite (`ActivateRateLimits=1` + `UserRateLimits=true` + a high provisioned limit), so every test transaction exercises the user-quota kernel paths. Happy to scope it to just the rate limit tests if preferred.
- Commit 4's bug should also be fixed on 26.04-main/26.05-main (same code).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
