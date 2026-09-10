# rondb-cli

MySQL queries, REST API calls, RonSQL queries
and Rondis commands, all in one database.

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

rondb> RONSQL SELECT COUNT(*) FROM string_keys
{"data":{"columns":["COUNT(*)"],"rows":[[1]]}}
(2.1ms)
```

## What is this?

rondb-cli is a unified command-line interface for RonDB that lets you:

- Run **Rondis commands** (GET, SET, INCR, etc.) - Redis protocol on RonDB
- Execute **SQL queries** (SELECT, INSERT, CREATE TABLE, etc.)
- Run **RonSQL queries** via REST API - high-performance SQL without MySQL overhead
- Use **REST API** for pk-read and batch operations
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
RonDB CLI - MySQL queries, REST API calls, RonSQL queries
and Rondis commands, all in one database.
Type .help for commands, Tab for autocomplete

[OK] Connected to RonDB 25.10

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
| `--rest-port` | `RONDB_REST_PORT` | 4406 | REST API port (RDRS2) |
| `--tls` | - | false | Enable TLS for all connections |

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

Once connected, you have multiple command types:

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

**MYSQL prefix** - explicit MySQL routing:
```
MYSQL SELECT * FROM table
```

**RonSQL queries** - high-performance SQL via REST API:
```
RONSQL SELECT * FROM table
RONSQL EXPLAIN SELECT * FROM table
RONSQL SET DATABASE mydb
```

**REST API** - pk-read and batch operations:
```
READ db.table col1, col2 FILTER pk_col=value
BATCH db.table: col1, col2 READ FILTER pk=1; READ FILTER pk=2;
```

**Internal commands** - dot prefix:
```
.browse              Open database browser (TUI)
.demo                Run a quick demo (write, read, query)
.tables              List all tables
.help                Show syntax help
.help internal       Show benchmark commands
.debug [0|1]         Toggle debug mode
.client [N]          Show/set client ID for benchmarks
.ronsql_database [db]     Show/set RonSQL database
.ronsql_format [format]   Show/set RonSQL output (JSON, JSON_ASCII, TEXT, TEXT_NOHEADER)
quit, exit, q        Exit the shell
```

### CLI Commands

```bash
rondb              # Start interactive shell
rondb status       # Check RonDB connectivity
rondb version      # Print version
rondb --help       # Show usage
```

### Hopsworks golden-query verification (A6)

The permanent RonDB-side runner consumes the committed JSON corpus produced by
the Java `HopsworksGoldenDump` utility in the Hopsworks tree. Java/Maven is not
needed to run it. This is separate from `.fs_verify --vectors`: it executes
captured Java `queryOnline` SQL, compares the reconstructed Go MySQL query with
that reference, and compares Go RonSQL vectors with the captured MySQL result.

Use a disposable RonDB cluster with MySQL and RDRS pointing at the same data.
The MySQL account needs CREATE/DROP DATABASE, CREATE TABLE, INSERT and SELECT
permissions; RDRS must be authorized to query the fixture databases. Known
engine defects can still cause errors or crashes; an all-green run is not
assumed.

The runner creates and drops the captured `golden_<featurestore-id>_fs`
databases, one fixture at a time. Existing databases are refused, never reused
or removed. Do not run golden jobs concurrently against the same cluster or
modify their databases while a job is running. It loads its own small,
versioned data set; do not use `.fs_load` for this mode.

From `tools/rondb-cli`, after building `rondb`:

```bash
# Start with one serving fixture. Adjust hosts, ports and authentication.
./rondb --host localhost --mysql-port 3306 --rdrs-port 4406 --no-rondis \
  -e ".fs_verify --golden internal/fsq/testdata/hopsworks_golden --fixture aggregate_single --json /tmp/a6-single-001.json"

# Run the complete corpus, using a different new report filename.
./rondb --host localhost --mysql-port 3306 --rdrs-port 4406 --no-rondis \
  -e ".fs_verify --golden internal/fsq/testdata/hopsworks_golden --json /tmp/a6-corpus-001.json --quiet"
```

Use `--mysql-host` and `--rdrs-host` for separate endpoints,
`RONDB_MYSQL_USER` / `RONDB_MYSQL_PASSWORD` for MySQL credentials and
`RONDB_RDRS_API_KEY` for RDRS authentication. TLS uses the normal CLI flags
`--tls` or `--rdrs-tls`. Do not disable MySQL or RDRS for a full comparison.

Golden-mode options (inside the `-e` command):

- `--golden DIR --json NEW_FILE` are required. The entire corpus manifest,
  file inventory and hashes are checked even when selecting one fixture.
  The report's parent directory must already exist. Existing report files
  and paths inside the corpus (including directory symlink aliases) are refused.
- `--fixture NAME` selects the exact fixture name, without `.json`.
- `--timeout 30s` bounds setup, connection/probe operations and each query;
  it is not a whole-run deadline. `--cleanup-timeout 30s` gives cleanup a
  separate deadline, including after cancellation.
- `--tolerance 1e-9` controls floating-point comparison tolerance.
- `--allow-reject` permits unexpected clean SQL rejections, reported
  separately. It never accepts malformed JSON, transport errors or mismatches.
- `--quiet` suppresses successful details, not failures or the summary.
  Vector-mode options such as `--db`, `--seed`, `--count` and `--threads`
  are not accepted; captured database names, deterministic keys and the
  fixed data clock are used.

The JSON report records selected fixtures and Java provenance, data version
and UTC reference time, bound SQL and query outcomes, both comparisons,
and setup/cleanup errors. After a started run it is written even when checks
fail; input/configuration failures before execution may produce no report.
An interrupted process or write failure can leave an empty or partial report;
use a new filename on retry.

Read `SUMMARY golden` as coverage counts, not a requirements certificate:
MySQL passes, RonSQL passes, rejections and untested requests are separate.
Definition/gate fixtures and empty statement sets run no SQL. DTOs without
RonSQL templates still compare both MySQL queries and report RonSQL UNTESTED.
A fixture can therefore pass without establishing RonSQL support.
Collect templates recognize the narrow existing F0 rejection; snowflake_binary
DTO 1 recognizes the observed F7 type-17 pass-through rejection. LEFT
missing-versus-NULL equivalence applies only to all-LEFT descendant subtrees.
Other failures remain errors. A failing or stopped/incomplete corpus run
returns nonzero with `-e`; a classification-only run may return zero but
explicitly reports no successful SQL comparisons.

On Ctrl-C/SIGTERM, the runner attempts bounded cleanup. Setup uncertainty,
connection-close failures or cleanup failures stop further fixtures.
Inspect `errors` and `remainingDatabases` in the report before retrying.
A failed CREATE may have succeeded on the server without acknowledgement;
that database is not claimed and may not appear in `remainingDatabases`.
Inspect the named database manually; never delete databases by a broad prefix.
Forced termination cannot guarantee cleanup.

A6 does not execute `queryOnlineScan` or the Hopsworks pk-read fallback and
does not establish E8 requirements acceptance. The first full user-run corpus
regression passed on 2026-09-10; unit-test confirmation and A6 MTR coverage
remain pending. For results and remaining verification, see
[the E4 plan](../../storage/ndb/claude_files/fs_ronsql/phase_e4.md#7-a6-implementation-and-user-verification)
and [the deferred-work list](../../mysql-test/suite/ronsql_fs/findings/BUGS_TODO.md).

### Benchmarks

rondb-cli includes built-in benchmarks for Rondis, SQL, and REST API (RDRS). Use `.help internal` to see all benchmark commands.

```
# Rondis benchmarks
.load_rondis [T] [N] [R]         Load test data (T threads, N ops, R rows/op)
.bench_rondis [T] [N] [R]        Read benchmark
.bench_rondis_cont [T] [N] [R] [W] [S]  Continuous benchmark (W% writes, S seconds)
.del_rondis [T] [N] [R]          Delete test data

# SQL benchmarks
.load_sql [T] [N] [R]            Load test data into SQL table
.bench_sql [T] [N] [R]           Read benchmark via SQL
.bench_sql_cont [T] [N] [R] [W] [S]    Continuous benchmark
.del_sql [T] [N] [R]             Delete test data
.drop_sql                        Drop the test table

# RDRS (REST API) benchmarks
.bench_rdrs [T] [N] [R]          Batch pk-read benchmark
.bench_rdrs_cont [T] [N] [R] [S] Continuous benchmark

# RonSQL benchmarks (pushdown aggregation / CTE queries over TPC-H data)
.load_tpch [SF] [T] [B]          Load TPC-H data (SF=scale factor)
.bench_ronsql                    List available RonSQL benchmark queries
.bench_ronsql <name> [T] [N]     Run a named query benchmark (default 1 thread x 10 requests)
.bench_ronsql all [T] [N]        Run all RonSQL benchmark queries sequentially
```

The RonSQL benchmarks include Feature-Store-style queries (`fs_*`: CTE feature
groups over one table or a simple join, joined to an entity table via scans and
key lookups) and TPC-H queries rewritten with CTEs (`tpch_q2`, `tpch_q11`,
`tpch_q13`, `tpch_q15`, `tpch_q22`). They require TPC-H data loaded via
`.load_tpch`. Latencies are reported with microsecond precision (min/avg/max,
p95/p99/p99.9). Note: if the `tpch` database was loaded with an older CLI
version, run `.drop_tpch` and reload to pick up the secondary indexes the
benchmarks use.

Key format: `bench:key:<client>:<thread>:<key>:<row>`

Use `.client N` to set a client ID prefix for running multiple benchmark instances.

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
│   │   ├── mysql.go        # MySQL client wrapper
│   │   └── rest.go         # REST API client (RDRS2)
│   ├── dsl/
│   │   └── parser.go       # DSL parser for READ/BATCH commands
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
- Four interfaces (MySQL, RonSQL, REST API, Rondis) = one powerful shell
