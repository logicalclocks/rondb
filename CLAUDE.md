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

### MySQL Test Suite
```bash
cd mysql-test
./mysql-test-run.pl [test_name]       # Run specific test
./mysql-test-run.pl --record test     # Record new baseline
./mysql-test-run.pl ndb_*             # NDB-specific tests
./mysql-test-run --extern test_name   # External server mode
```

Test files: `t/*.test` (input), `r/*.result` (expected output)

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
- `READ` / `BATCH` → REST API (pk-read/pk-delete via RDRS)
- Everything else → Rondis client

See `tools/rondb-cli/CLAUDE.md` for detailed rondb-cli documentation.

### RonDB Core Structure
- `/storage/ndb/` - NDB Cluster storage engine
- `/storage/ndb/rest-server2/` - REST + Rondis server (RDRS)
- `/client/` - MySQL CLI tools
- `/mysql-test/` - Integration test suite

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

## Security Notes

- `mysql.go` includes SQL injection prevention via identifier sanitization
- TLS support for both MySQL and Rondis connections
- Never commit `.env` files or credentials
