# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RonDB is a distribution of NDB Cluster (MySQL's distributed key-value store with SQL capabilities). This repository contains:
1. **RonDB Core** - C++ codebase (MySQL/NDB Cluster fork)
2. **rondb-cli** - Go CLI tool at `/tools/rondb-cli/` for unified Rondis + SQL access

The unique value proposition: Write data with Rondis (Redis protocol), query it with SQL, same data store.

## Build Commands

### rondb-cli (Go tool)
```bash
cd tools/rondb-cli
go build -o rondb .
```

### RonDB Core (C++ - requires CMake)
```bash
cmake -DWITH_NDB=1 -DWITH_RDRS=1 -DWITH_NDB_TEST=1 -DWITH_UNIT_TESTS=1 \
      -DWITH_SSL=$OPENSSL_ROOT -DWITH_ROUTER=0 .
make -j$(nproc)
```

Key CMake flags:
- `-DWITH_NDB=1` - NDB storage engine
- `-DWITH_RDRS=1` - REST server + Rondis support
- `-DWITH_NDB_TEST=1` - NDB test suite

## Testing

### MySQL Test Suite (MTR)
```bash
cd mysql-test
./mtr [test_name]                     # Run specific test
./mtr --record test                   # Record new baseline
./mtr --suite=rondis                  # Run all Rondis tests
./mtr --suite=rondis rondis_basic     # Run specific Rondis test
./mtr ndb_*                           # NDB-specific tests
./mtr --extern test_name              # External server mode
```

Test files: `t/*.test` (input), `r/*.result` (expected output)

### Rondis MTR Tests
Located in `mysql-test/suite/rondis/`:
- `t/rondis_basic.test` - Basic Redis commands (GET, SET, HSET, etc.)
- `t/rondis_advanced.test` - Advanced operations (GETRANGE, SETRANGE, INCR/DECR)
- `r/*.result` - Expected output files

Tests use `rondb-cli` with `--quiet` flag for scripted execution via `--exec`.

**rondis_advanced.test Coverage:**

| Test | Command | Expected Result |
|------|---------|-----------------|
| STRLEN | `STRLEN key` | 13 (length of "Hello, World!") |
| STRLEN | `STRLEN nonexistent` | 0 (non-existent key) |
| GETRANGE | `GETRANGE key 0 4` | "Hello" (inclusive indices) |
| GETRANGE | `GETRANGE key -6 -1` | "World!" (negative indices from end) |
| SETRANGE | `SETRANGE key 6 'Redis'` | 11 (new string length) |
| SETRANGE | Padding beyond length | `Hi\0\0\0There` (null byte padding) |
| INCR/DECR | Start from zero | 1, -1 (creates key if not exists) |
| INCRBY/DECRBY | Large values | 1000000, 500000 |
| HSET | Multiple fields | Returns count of fields set |
| HINCRBY/HDECRBY | Hash field increments | Correct arithmetic |
| HMGET | Mixed existing/nil | Array with (nil) for missing fields |

**Running Rondis tests:**
```bash
./mtr --suite=rondis                    # Run all Rondis tests
./mtr --suite=rondis rondis_advanced    # Run specific test
./mtr --record --suite=rondis test_name # Record new baseline
./mtr --suite=rondis test --nowarnings  # Ignore transient NDB warnings
```

### rondb-cli Manual Testing
```bash
cd tools/rondb-cli
./scripts/start-rondb.sh     # Start local cluster via rondb-docker
./rondb status               # Check connectivity
./rondb                      # Interactive shell
# In shell: .demo, .bench [N]
./scripts/cleanup-rondb.sh   # Stop and clean up
```

## Architecture

### rondb-cli Structure
```
tools/rondb-cli/
├── cmd/root.go           # Cobra commands (rondb, status, version)
├── internal/
│   ├── client/
│   │   ├── rondis.go     # Redis protocol client (go-redis/v9)
│   │   └── mysql.go      # MySQL client with SQL injection protection
│   ├── shell/repl.go     # Interactive REPL - command detection/dispatch
│   ├── tui/browser.go    # Database browser UI
│   └── ui/               # Terminal styling (lipgloss)
```

Command flow: `cmd/root.go` → `shell/repl.go` → `client/*.go`

Shell detects command type:
- `.` prefix → internal commands (.demo, .bench, .browse, .tables, .help)
- SQL keywords (SELECT, INSERT, etc.) → MySQL client
- `READ` / `BATCH` → REST API (pk-read/pk-delete/pk-write via RDRS)
- `RONSQL` → REST API RonSQL queries
- Everything else → Rondis client

See `tools/rondb-cli/CLAUDE.md` for detailed rondb-cli documentation.

### RonDB Core Structure
- `/storage/ndb/` - NDB Cluster storage engine
- `/storage/ndb/rest-server2/` - REST + Rondis server (RDRS)
- `/storage/ndb/src/rondis/` - Rondis Redis-compatible server implementation
- `/client/` - MySQL CLI tools
- `/mysql-test/` - Integration test suite

### Pushdown Join Aggregation (RONDB-733)
Design and implementation docs in `storage/ndb/claude_files/pushdown_join_aggregation/`:
- `CLAUDE.md` — DBLQH block overview and join aggregation context
- `local_database_research.md` — Architecture: signal flow, shared state, aggregation engine
- `local_database_implementation.md` — DBLQH implementation plan (Phases 1-6, complete)
- `coordinator.md` — DBSPJ protocol and aggregate design
- `coordinator_research.md` — DBTC orchestration plan (Phase 7: SETUP/COMPLETE/RELEASE)
- `coordinator_implementation.md` — DBSPJ implementation notes (Phase 7: aggStateKeys, lookup_send)

### Rondis Server Implementation
```
storage/ndb/src/rondis/
├── src/
│   ├── commands.cc        # Redis command implementations (GET, SET, GETRANGE, etc.)
│   ├── db_operations.cc   # NDB database operations for Rondis
│   ├── interpreted_code.cc # NDB interpreted code for atomic operations
│   ├── rondb.cc           # Command dispatch and routing
│   └── redis_parser.cc    # RESP protocol parser
├── include/
│   ├── table_definitions.h # KeyStorage struct, table schemas
│   └── db_operations.h    # Database operation declarations
```

Key patterns:
- Commands use NDB interpreted code for atomic read-modify-write operations
- `KeyStorage` struct tracks key state through operations
- RESP protocol: `$N\r\n` for bulk strings, `*N\r\n` for arrays, `:N\r\n` for integers
- Single-key GET returns bulk string; MGET returns array

## Configuration

rondb-cli uses flags and environment variables:
- `--host` / `RONDB_HOST` (default: localhost)
- `--rondis-port` / `RONDB_RONDIS_PORT` (default: 6379)
- `--mysql-port` / `RONDB_MYSQL_PORT` (default: 3306)
- `--tls` - Enable TLS for both connections
- `RONDB_MYSQL_USER` / `RONDB_MYSQL_PASSWORD` - Auth credentials

## Design Philosophy

From `tools/rondb-cli/docs/PHILOSOPHY.md`:

**3-Minute Win**: Connect (30s) → Diagnose (30s) → Decide (2min)

**Key constraints**:
- Silent ≠ successful - every command produces output
- Human-first output, never cryptic
- Fail loud with context explaining *why* and *what to do*
- No magic - explicit about everything

**Graceful degradation**: Rondis is optional (SQL-only mode if unavailable), MySQL is required.

## Rondis Data Model

Rondis stores Redis data in NDB tables. The `redis_0` database contains tables like `string_keys` that can be queried via SQL:
```sql
SELECT redis_key, value FROM redis_0.string_keys WHERE redis_key LIKE 'user:%'
```

## NDB Important Notes

- **Always create NDB tables through MySQL** (CREATE TABLE ... ENGINE=NDB), never through the NDB API directly. Tables created via the NDB API are not visible to MySQL servers. If you need table metadata (tableId, schemaVersion) for signal-level tests, create the table via MySQL first, then look up metadata via the NDB dictionary API.

## Security Notes

- `mysql.go` includes SQL injection prevention via identifier sanitization
- TLS support for both MySQL and Rondis connections
- Never commit `.env` files or credentials

## Rondis Implementation Notes

**RESP Protocol Compliance:**
- Single-key GET/HGET must return bulk string (`$N\r\n...`), not array
- MGET/HMGET return array format (`*N\r\n$N\r\n...`)
- GETRANGE uses inclusive indices (0-4 returns 5 characters)
- DEL returns count of deleted keys (0 if key doesn't exist)

**NDB Interpreted Code:**
- Used for atomic read-modify-write operations (SETRANGE, INCR, etc.)
- `load_op_type()` detects INSERT vs UPDATE for writeTuple operations
- `read_full()` reads column into interpreter memory with 4-byte header
- `write_from_mem()` writes from interpreter memory to column

**Common Gotchas:**
- Hash keys (HSET) use different redis_key_id than string keys
- `rondb-cli` must handle `redis.Nil` error as valid "(nil)" response
- MTR tests need `--quiet` flag to suppress timing output
- STRLEN must initialize `tot_value_len = 0` before NDB execute (non-existent keys leave struct uninitialized)
- SETRANGE pads with null bytes (`\0`), not spaces - rondb-cli displays these as `\0` escape sequences
- NDB schema distribution timeouts can cause spurious MTR failures - use `--nowarnings` if needed
