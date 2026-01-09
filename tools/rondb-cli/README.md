# rondb-cli

Rondis commands. SQL queries. One database.

```
rondb> SET user:123 '{"name":"Alice","age":30}'
OK (0.8ms)

rondb> SELECT * FROM redis_0.string_keys WHERE redis_key LIKE 'user:%'
┌───────────┬─────────────────────────────┐
│ redis_key │ value                       │
├───────────┼─────────────────────────────┤
│ user:123  │ {"name":"Alice","age":30}   │
└───────────┴─────────────────────────────┘
1 row (1.2ms)
```

## What is this?

rondb-cli is a unified command-line interface for RonDB that lets you:

- Run Rondis commands (GET, SET, INCR, etc.) - Redis protocol on RonDB
- Execute SQL queries (SELECT, INSERT, CREATE TABLE, etc.)
- Query Rondis data with SQL (the magic)

All in one shell. No context switching.

## Installation

### Build from source

```bash
cd tools/rondb-cli
go build -o rondb .
```

### Install globally

```bash
sudo cp rondb /usr/local/bin/
```

### Run

```bash
rondb              # Start interactive shell
rondb status       # Check connection to RonDB
rondb version      # Show version
```

## Quick Start

### 1. Start RonDB

```bash
./scripts/start-rondb.sh
```

This clones [rondb-docker](https://github.com/logicalclocks/rondb-docker) and starts a local cluster with:
- MySQL on port 3306
- Rondis (Redis protocol) on port 6379
- REST API on port 4406

To stop and clean up:

```bash
./scripts/cleanup-rondb.sh        # Stop containers, remove volumes
./scripts/cleanup-rondb.sh --all  # Also remove cloned rondb-docker repo
```

### 2. Connect

```bash
rondb
```

You'll see:
```
RonDB CLI - Rondis commands. SQL queries. One database.
Type .help for commands, Tab for autocomplete

[OK] Connected to RonDB 24.10

rondb>
```

### 3. Try the demo

```
rondb> .demo
```

This runs a quick demo showing Rondis writes, reads, and SQL queries with timing.

### 4. Play

Rondis commands work directly:
```
rondb> SET hello world
OK (0.5ms)

rondb> GET hello
"world" (0.3ms)
```

SQL queries work directly:
```
rondb> SELECT * FROM redis_0.string_keys
┌───────────┬───────┐
│ redis_key │ value │
├───────────┼───────┤
│ hello     │ world │
└───────────┴───────┘
1 row (1.1ms)
```

## Configuration

### Connection

| Flag | Env Variable | Default | Description |
|------|--------------|---------|-------------|
| `--host` | `RONDB_HOST` | localhost | RonDB host |
| `--rondis-port` | `RONDB_RONDIS_PORT` | 6379 | Rondis port |
| `--mysql-port` | `RONDB_MYSQL_PORT` | 3306 | MySQL port |
| `--tls` | - | false | Enable TLS (MySQL + Rondis) |

### Authentication

| Env Variable | Default | Description |
|--------------|---------|-------------|
| `RONDB_MYSQL_USER` | root | MySQL username |
| `RONDB_MYSQL_PASSWORD` | (empty) | MySQL password |

### Examples

```bash
# Connect to remote cluster
RONDB_HOST=db.example.com \
RONDB_MYSQL_USER=admin \
RONDB_MYSQL_PASSWORD=secret \
rondb

# Connect with TLS
rondb --host db.example.com --tls
```

## Commands

### Interactive Shell

Once connected, you have three command types:

**Rondis commands** (Redis protocol):
```
SET key value
GET key
DEL key
MGET key1 key2
INCR counter
HSET hash field value
HGET hash field
KEYS pattern
```

**SQL queries** - standard MySQL syntax:
```
SELECT * FROM table
INSERT INTO table VALUES (...)
CREATE TABLE ...
SHOW TABLES
DESCRIBE table
USE database
```

**Internal commands** - dot prefix:
```
.browse     Open database browser (TUI)
.demo       Run a quick demo (write, read, query)
.bench [N]  Run benchmark (default 1000 ops, shows throughput)
.tables     List all tables
.help       Show help
.quit       Exit
```

### CLI Commands

```bash
rondb              # Start interactive shell
rondb status       # Check RonDB connectivity
rondb version      # Print version
rondb --help       # Show usage
```

## The Magic: SQL on Rondis Data

RonDB stores Rondis data in NDB tables. This means you can:

1. Write with Rondis (fast, simple):
```
SET user:123 '{"name":"Alice","email":"alice@example.com"}'
SET user:456 '{"name":"Bob","email":"bob@example.com"}'
```

2. Query with SQL (powerful, flexible):
```sql
SELECT redis_key, value_start
FROM redis_0.string_keys
WHERE redis_key LIKE 'user:%'
```

3. Join with application tables:
```sql
SELECT u.name, o.total
FROM users u
JOIN redis_0.string_keys r ON r.redis_key = CONCAT('cart:', u.id)
```

## Project Structure

```
rondb-cli/
├── main.go                 # Entry point
├── cmd/
│   └── root.go             # Cobra commands
├── internal/
│   ├── client/
│   │   ├── rondis.go       # Rondis client wrapper
│   │   └── mysql.go        # MySQL client wrapper
│   ├── shell/
│   │   └── repl.go         # Interactive shell with readline
│   ├── tui/
│   │   └── browser.go      # Database browser UI
│   └── ui/
│       ├── colors.go       # Terminal styling
│       └── table.go        # Table formatting
├── docs/
│   └── PHILOSOPHY.md       # Project principles
└── scripts/
    ├── start-rondb.sh      # Start local RonDB
    └── cleanup-rondb.sh    # Stop and clean up
```

## Development

### Prerequisites

- Go 1.21+
- Docker (for local RonDB)

### Build

```bash
go build -o rondb .
```

### Test manually

```bash
# Start RonDB
./scripts/start-rondb.sh

# In another terminal
rondb status
rondb
```

### Dependencies

- [cobra](https://github.com/spf13/cobra) - CLI framework
- [readline](https://github.com/chzyer/readline) - Interactive input with history
- [go-redis](https://github.com/redis/go-redis) - Redis client (for Rondis protocol)
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) - MySQL driver
- [tablewriter](https://github.com/olekukonko/tablewriter) - Table formatting
- [lipgloss](https://github.com/charmbracelet/lipgloss) - Terminal styling

## Philosophy

See [docs/PHILOSOPHY.md](docs/PHILOSOPHY.md) for design principles.

TL;DR:
- 3-minute win: connect, diagnose, decide
- One CLI, no ceremony
- Rondis speed + SQL power = the aha moment
