# RONDB-1039: MySQL Router in RDRS — Phase Plan

## Completed Phases

### Phase 1: Pure MySQL protocol proxy
MySQL wire protocol proxy in RDRS — handshake relay, command forwarding, response relay.

### Phase 2: Route SELECT queries to RonSQL
Intercept SELECT queries outside transactions and try RonSQL first, fall back to backend mysqld on failure.

### Phase 3: Transaction-aware routing
Track transaction state (BEGIN/COMMIT/ROLLBACK, SERVER_STATUS_IN_TRANS) to skip RonSQL during transactions.

### Phase 4: myrouter MTR test suite + protocol fixes
Basic myrouter test suite, fixed protocol issues found during testing.

### Phase 5: myrouter-as-default MTR flag
`myrouter-as-default=true` in .cnf redirects default MTR connection through the router.

### Phase 6: myrouter_ndb suite
New `myrouter_ndb` suite that runs real NDB tests through the router. Fixed node ID race (Dedicated=1).

### Phase 7: Debug logging
`DebugLogging` config option, logs PROXY/RONSQL/FALLBACK routing decisions with query text and reason.

### Phase 8: Query classification
`may_be_aggregate_query()` pre-classifies SELECTs — non-aggregate queries skip RonSQL entirely.

### Phase 9: Expand myrouter_ndb test coverage
Added join_pushdown_default and join_pushdown_bka from ndb_opt suite.

### Phase 10: Protocol hardening
- COM_STMT_PREPARE: read param definitions + EOF + column definitions + EOF
- COM_STMT_CLOSE: no server response (set_is_reply(false))
- LOCAL INFILE: relay file data from client → backend, then read final OK/ERR
- Multi-result sets: loop on SERVER_MORE_RESULTS_EXIST after EOF/OK
- COM_RESET_CONNECTION / COM_CHANGE_USER: reset in_transaction_ and current_database_

## Remaining Phases

### Phase 11: More myrouter_ndb tests
Add more ndb_opt tests to myrouter_ndb: condition_pushdown, ndb_index, ndb_alter_table, subquery tests, etc.

### Phase 12: Prepared statement support for RonSQL
Currently all prepared statements proxy to backend. Teach RonSQL to handle COM_STMT_PREPARE/EXECUTE for aggregate queries so they benefit from RonSQL routing.

### Phase 13: Metrics and monitoring
Expose routing statistics (PROXY/RONSQL/FALLBACK counts, latency percentiles) through the RDRS REST health endpoint.

## Rejected Ideas
- **Backend connection pooling**: Too risky — hard to ensure shared connections haven't had config changed
- **Multi-backend / read-write splitting**: No value for RonDB since all clients can read and write
