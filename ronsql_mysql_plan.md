# Investigation: RonSQL with MySQL Protocol Support

## Context

RonSQL is a standalone SQL engine in RDRS that parses SQL (flex/bison), plans queries, and executes directly via the NDB API — bypassing mysqld entirely. Currently it is only accessible via HTTP/REST (`POST /0.x.0/ronsql`) and a CLI tool (`ronsql_cli`).

**Goal**: Make RonSQL accessible via the MySQL wire protocol as a **hybrid router** — RonSQL handles SELECT queries natively (with pushdown aggregation), and everything else (DDL, DML, SET, SHOW, USE, metadata queries) is proxied to a real mysqld in the same cluster. This enables:
1. **Full MTR compatibility** — run existing NDB/MySQL test cases unmodified; DDL/DML goes to mysqld, SELECTs are handled by RonSQL
2. **Standard tooling** — any MySQL client can connect and use the full SQL feature set
3. **Transparent acceleration** — applications get RonSQL's pushdown aggregation for SELECTs without any code changes
4. **Correctness testing** — compare RonSQL SELECT results against mysqld results on the same data

## Current Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              RDRS (rdrs2)                    │
                    │                                             │
  HTTP/REST ───────►│  Drogon HTTP  ──► ronsql_ctrl ──► ronsql_dal│
  (port 4406)       │  (64 threads)                               │
                    │                      │                      │
                    │              RonSQLPreparer                  │
                    │         (parse → plan → compile → execute)  │
                    │                      │                      │
  Redis ───────────►│  Pink/Rondis ──► rondb_redis_handler        │
  (port 6379)       │  (16 threads)        │                      │
                    │                      ▼                      │
                    │              NDB Connection Pool             │
                    │              (thread-local Ndb*)             │
                    └──────────────────────┬──────────────────────┘
                                           │
                                    NDB Cluster (data nodes)
```

## Proposed Architecture: Hybrid Router

```
                         ┌──────────────────────────────────────────┐
                         │              RDRS (rdrs2)                 │
                         │                                          │
  MySQL client ─────────►│  MySQL Protocol Listener (port 3307)     │
                         │         │                                │
                         │    ┌────▼─────┐                          │
                         │    │  Router   │ ◄── classify query      │
                         │    └──┬────┬───┘                          │
                         │       │    │                              │
                         │  SELECT  Everything else                  │
                         │       │    │                              │
                         │       ▼    ▼                              │
                         │  RonSQL   MySQL Backend                   │
                         │  (NDB    Connection                       │
                         │   API)   (proxy to mysqld:3306)           │
                         │       │    │                              │
                         │       ▼    │                              │
                         │  NDB Pool  │                              │
                         └───────┬────┼──────────────────────────────┘
                                 │    │
                          NDB Cluster  mysqld (port 3306)
```

**Key design**: Each client connection to the RonSQL MySQL port maintains a **backend connection to a real mysqld**. The router inspects incoming queries and decides:
- **SELECT** → execute via RonSQLPreparer (pushdown aggregation, direct NDB API)
- **Everything else** (INSERT, UPDATE, DELETE, CREATE, SET, SHOW, USE, etc.) → forward to mysqld, relay response back to client

### Why Hybrid Router?

This makes RonSQL a **transparent MySQL-compatible accelerator**:
- MTR tests work unmodified — DDL creates tables via mysqld, DML inserts data via mysqld, SELECTs are handled by RonSQL
- SET commands configure the mysqld session (and potentially trigger RonSQL-side actions)
- SHOW/DESCRIBE/EXPLAIN work via mysqld fallback
- Applications need zero changes — just point at a different port

## Detailed Design

### 1. MySQL Wire Protocol Layer (~1500 lines)

**Client-facing protocol (front-end):**

The RonSQL MySQL listener speaks the standard MySQL wire protocol to clients:

- **Handshake**: Server greeting → client auth response → OK/ERR
- **Command loop**: Read COM_xxx packets, dispatch to router
- **Result encoding**: Column definitions + row data + EOF/OK packets
- **Packet framing**: 3-byte length + 1-byte sequence number

Commands handled:
| Command | Routing |
|---------|---------|
| COM_QUERY | Router decides: SELECT → RonSQL, else → mysqld proxy |
| COM_QUIT | Close both client and backend connections |
| COM_PING | Return OK directly (no proxy needed) |
| COM_INIT_DB | Forward to mysqld backend + update RonSQL database context |
| COM_STMT_PREPARE | Forward to mysqld backend |
| COM_STMT_EXECUTE | Forward to mysqld backend |
| COM_STMT_CLOSE | Forward to mysqld backend |
| Others | Forward to mysqld backend |

### 2. Query Router (~300 lines)

The router needs to classify COM_QUERY statements quickly:

**Fast classification approach:**
1. Skip leading whitespace and comments (`/* ... */`, `-- ...`)
2. Check first keyword:
   - `SELECT` → candidate for RonSQL (with fallback, see below)
   - `WITH` (CTE) → if RonSQL supports it, route to RonSQL; else mysqld
   - Everything else → mysqld

**RonSQL fallback**: If RonSQL fails to parse or execute a SELECT (e.g., uses features RonSQL doesn't support like window functions, UNION, etc.), catch the error and transparently re-route the query to mysqld. This ensures no query ever fails just because of routing.

```
COM_QUERY arrives
    │
    ▼
classify_query(sql)
    │
    ├── SELECT/WITH → try RonSQL
    │                    │
    │               success? ──► send MySQL result packets to client
    │                    │
    │                 failure ──► forward to mysqld (transparent fallback)
    │
    └── else ──► forward to mysqld ──► relay response to client
```

**SET command handling:**
SET commands are always forwarded to mysqld. Some may also trigger RonSQL-side actions:
- `SET NAMES ...` / `SET character_set_results = ...` → update charset context for RonSQL result encoding
- `USE database` (COM_INIT_DB or `USE db` query) → update both mysqld backend and RonSQL database context

### 3. MySQL Backend Connection (proxy) (~800 lines)

Each client connection maintains a persistent TCP connection to the real mysqld:

**Connection lifecycle:**
1. When client connects to RonSQL MySQL port (3307), establish backend connection to mysqld (3306)
2. Authenticate to mysqld using configured credentials (or relay client credentials)
3. Forward COM_INIT_DB to set the same database on backend
4. Backend connection lives as long as client connection

**Proxy forwarding:**
For non-SELECT queries, the proxy:
1. Sends the raw COM_QUERY packet to mysqld backend
2. Reads the complete response (could be OK, ERR, or result set)
3. Relays the response packets back to the client, adjusting sequence numbers

**Response relay:**
MySQL responses are self-delimiting:
- **OK packet**: Single packet (starts with 0x00)
- **ERR packet**: Single packet (starts with 0xFF)
- **Result set**: Column count → column defs → EOF → rows → EOF/OK
- **LOCAL INFILE**: Starts with 0xFB (can forward or reject)

The proxy reads the first byte to determine response type, then reads the appropriate number of packets.

### 4. RonSQL Result → MySQL Protocol Encoding (~500 lines)

When RonSQL handles a SELECT, results must be encoded as MySQL protocol packets.

**Column metadata**: RonSQL already knows column names and NDB types from the dictionary. Map to MySQL types:
| NDB Type | MySQL Protocol Type | Field Type Code |
|----------|-------------------|-----------------|
| Int/Unsigned | MYSQL_TYPE_LONG | 0x03 |
| Bigint | MYSQL_TYPE_LONGLONG | 0x08 |
| Float | MYSQL_TYPE_FLOAT | 0x04 |
| Double | MYSQL_TYPE_DOUBLE | 0x05 |
| Decimal | MYSQL_TYPE_NEWDECIMAL | 0xF6 |
| Varchar/Char | MYSQL_TYPE_VAR_STRING | 0xFD |
| Blob/Text | MYSQL_TYPE_BLOB | 0xFC |
| Date | MYSQL_TYPE_DATE | 0x0A |
| Datetime | MYSQL_TYPE_DATETIME | 0x0C |
| Timestamp | MYSQL_TYPE_TIMESTAMP | 0x07 |

**Row encoding**: MySQL text protocol encodes each column value as a length-encoded string. NULL is encoded as 0xFB. This aligns well with RonSQL's TEXT output format — both are string representations.

**Approach**: Add a new `OutputFormat::MYSQL_ROWS` to `RonSQLExecParams` where instead of writing to an ostream, RonSQL populates a vector of row buffers that the MySQL protocol handler encodes into packets. Alternatively, use a callback interface where RonSQL calls `on_column_def()` and `on_row()` methods.

### 5. Per-Connection State (~200 lines)

```cpp
struct MySQLConnectionState {
    int client_fd;              // Client socket
    int backend_fd;             // mysqld backend socket
    uint32_t connection_id;     // Unique connection ID
    uint8_t sequence_nr;        // Packet sequence number
    std::string database;       // Current database
    uint32_t client_capabilities; // Negotiated capabilities
    char read_buf[16384];       // Read buffer
    char write_buf[65536];      // Write buffer (for result sets)
};
```

### 6. Configuration (~200 lines)

Add to `config_structs_def.hpp`:
```cpp
CLASS(MySQLProtocol,
  CM(bool, enable, Enable, false, "Enable MySQL protocol frontend for RonSQL")
  CM(std::string, serverIP, ServerIP, "0.0.0.0", "Bind address")
  CM(Uint32, serverPort, ServerPort, 3307, "MySQL protocol listen port")
  CM(Uint32, numThreads, NumThreads, 16, "Worker threads")
  CM(std::string, backendHost, BackendHost, "127.0.0.1", "Backend mysqld host")
  CM(Uint32, backendPort, BackendPort, 3306, "Backend mysqld port")
  CM(std::string, backendUser, BackendUser, "root", "Backend mysqld user")
  CM(std::string, backendPassword, BackendPassword, "", "Backend mysqld password")
)
```

## MTR Integration

### How It Works with MTR

MTR already starts a full NDB cluster (ndb_mgmd + ndbmtd + mysqld + RDRS). With MySQL protocol enabled in RDRS:

1. MTR starts the cluster as normal (mysqld on port X, RDRS on port Y)
2. RDRS's MySQL protocol listens on port Z (configured in RDRS JSON config)
3. MTR tests can connect to port Z as if it were another mysqld
4. DDL/DML flows through to the real mysqld (port X) via proxy
5. SELECTs are handled by RonSQL with pushdown aggregation

### MTR Test Approaches

**Approach A: Dual-connection comparison tests (new suite)**
```sql
-- Connect to real mysqld
--connect(mysql_conn, localhost, root, , test, $MASTER_MYPORT)
-- Connect to RonSQL MySQL port
--connect(ronsql_conn, localhost, root, , test, $RONSQL_MYPORT)

-- Create table and insert data via mysqld
--connection mysql_conn
CREATE TABLE t1 (a INT, b INT) ENGINE=NDB;
INSERT INTO t1 VALUES (1,10), (2,20), (3,30);

-- Query via RonSQL and compare
--connection ronsql_conn
SELECT SUM(b) FROM t1 WHERE a > 1;
-- Expected: 50 (same as mysqld would return)

-- Query via mysqld for comparison
--connection mysql_conn
SELECT SUM(b) FROM t1 WHERE a > 1;
-- Expected: 50

-- Cleanup via mysqld
--connection mysql_conn
DROP TABLE t1;
```

**Approach B: Run existing tests through RonSQL port**
Many existing `ndb_*` tests create tables, insert data, and then SELECT. Because the router proxies DDL/DML to mysqld, these tests can run unmodified against the RonSQL port. The only difference is that SELECTs go through RonSQL instead of mysqld.

This is powerful: if a test passes through mysqld but fails through RonSQL, it immediately identifies a RonSQL correctness issue.

**Approach C: Transparent re-run**
Create a wrapper that re-runs an existing MTR test but connects to the RonSQL port instead of mysqld. Any difference in output reveals a RonSQL bug.

### MTR Suite Configuration

```ini
# mysql-test/suite/ronsql-mysql/my.cnf
[rdrs]
# Enable MySQL protocol in RDRS config
# RonSQL MySQL port is exposed as RONSQL_MYPORT in test environment
```

## What RonSQL Already Supports (handled natively)

- SELECT with JOINs, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
- Aggregations: COUNT, SUM, AVG, MIN, MAX (with pushdown)
- Subqueries (EXISTS, IN, scalar, correlated scalar)
- CASE expressions, arithmetic in aggregates
- Data types: INT variants, FLOAT, DOUBLE, DECIMAL, VARCHAR, CHAR, BLOB/TEXT, DATE/TIME

## What Gets Proxied to mysqld

- DML: INSERT, UPDATE, DELETE
- DDL: CREATE TABLE, ALTER TABLE, DROP TABLE
- SET statements (with optional RonSQL-side effects for charset/database)
- SHOW commands (SHOW TABLES, SHOW DATABASES, SHOW CREATE TABLE, etc.)
- USE database (also updates RonSQL database context)
- Prepared statements (COM_STMT_PREPARE/EXECUTE)
- Transactions (BEGIN, COMMIT, ROLLBACK)
- Information_schema queries
- DESCRIBE, EXPLAIN
- Any SELECT that RonSQL can't handle (transparent fallback)

## Effort Estimate

| Component | Complexity | Lines |
|-----------|-----------|-------|
| MySQL wire protocol (handshake, packets, framing) | Medium | ~1500 |
| Query router (classify + fallback logic) | Low-Medium | ~300 |
| MySQL backend proxy (connect, forward, relay) | Medium | ~800 |
| RonSQL result → MySQL protocol encoding | Medium | ~500 |
| NDB type → MySQL type mapping | Medium | ~300 |
| Configuration & thread pool setup | Low | ~200 |
| Per-connection state management | Low | ~200 |
| Error mapping (RonSQL → MySQL error packets) | Low | ~100 |
| MTR integration (suite, config, test infrastructure) | Medium | ~500 |
| **Total** | | **~4400 lines** |

## Phased Implementation Plan

### Phase 1: Pure Proxy (foundation)
- MySQL protocol listener in RDRS (handshake, packet framing)
- Backend connection to mysqld
- Forward ALL commands to mysqld and relay responses
- Test: connect with `mysql` CLI, run any SQL → everything works via proxy
- **Value**: Validates the protocol implementation end-to-end

### Phase 2: SELECT Routing to RonSQL
- Query classifier (detect SELECT statements)
- Route SELECT to RonSQLPreparer
- Encode RonSQL results as MySQL protocol packets
- Transparent fallback: if RonSQL fails, retry via mysqld proxy
- Test: `SELECT 1+1`, `SELECT * FROM t1`, aggregation queries

### Phase 3: Session State Synchronization
- COM_INIT_DB updates both backend and RonSQL database context
- SET NAMES/charset changes reflected in RonSQL result encoding
- USE database handled on both sides
- Test: multi-database queries, charset-sensitive data

### Phase 4: MTR Integration
- New `ronsql-mysql` MTR suite
- Configure RDRS MySQL port in MTR environment
- Comparison tests: same query via mysqld vs RonSQL
- Re-run selected existing `ndb_*` tests through RonSQL port
- Test: `./mtr --suite=ronsql-mysql` passes

### Phase 5: Robustness & Production Readiness
- Authentication (relay client credentials to backend, or separate auth)
- TLS support (reuse RDRS TLS config)
- Connection limits, timeouts, keepalive
- Metrics (queries routed to RonSQL vs proxied to mysqld)
- Graceful shutdown (drain connections)

## Risks and Considerations

1. **Sequence number management**: MySQL protocol uses per-command sequence numbers. When proxying, we relay backend responses with adjusted sequence numbers. When routing to RonSQL, we generate our own sequence numbers. Must be careful not to mix these up.

2. **Transaction state**: If a client does `BEGIN; INSERT ...; SELECT ...;`, the SELECT should see the uncommitted INSERT. Since the INSERT goes to mysqld and the SELECT goes to RonSQL (which reads from NDB), there could be visibility issues. **Mitigation**: When inside an explicit transaction, route ALL queries (including SELECT) to mysqld. Only route SELECTs to RonSQL in autocommit mode.

3. **Prepared statements with SELECT**: A client may prepare a SELECT and then execute it. Since COM_STMT_PREPARE goes to mysqld, the execute must also go to mysqld. **Mitigation**: All prepared statement commands go to mysqld unconditionally.

4. **Result set compatibility**: RonSQL may format values slightly differently than mysqld (e.g., DECIMAL precision, date formatting). The comparison tests in Phase 4 will catch these differences.

5. **Backend connection management**: Each client connection needs a backend mysqld connection. This doubles the connection count on mysqld. For testing this is fine; for production, connection pooling could be added later.

6. **RonSQL fallback latency**: If RonSQL fails on a SELECT and we fall back to mysqld, the client experiences extra latency (RonSQL attempt + mysqld re-execution). This should be rare once RonSQL is mature, and the fallback ensures correctness.

## Key Files to Create

| File | Purpose |
|------|---------|
| `storage/ndb/src/ronsql/mysql_protocol.h` | MySQL protocol handler class |
| `storage/ndb/src/ronsql/mysql_protocol.cc` | Protocol implementation (handshake, packets, encoding) |
| `storage/ndb/src/ronsql/mysql_router.h` | Query classifier and routing logic |
| `storage/ndb/src/ronsql/mysql_router.cc` | Router implementation |
| `storage/ndb/src/ronsql/mysql_proxy.h` | Backend mysqld proxy connection |
| `storage/ndb/src/ronsql/mysql_proxy.cc` | Proxy implementation (connect, forward, relay) |
| `mysql-test/suite/ronsql-mysql/` | MTR test suite |

## Key Files to Modify

| File | Change |
|------|--------|
| `storage/ndb/rest-server2/server/src/config_structs_def.hpp` | Add MySQLProtocol config class |
| `storage/ndb/rest-server2/server/src/main.cc` | Start MySQL protocol listener thread |
| `storage/ndb/rest-server2/server/src/rdrs_dal.cpp` | Entry point for MySQL protocol → ronsql_dal |
| `storage/ndb/src/ronsql/RonSQLPreparer.hpp` | Result callback/visitor interface |
| `storage/ndb/src/ronsql/ResultPrinter.hpp/cpp` | MySQL protocol result encoding |
| Various `CMakeLists.txt` | Build integration |
| `mysql-test/mysql-test-run.pl` | Expose RONSQL_MYPORT variable |

## Verification

- **Phase 1**: `mysql -h 127.0.0.1 -P 3307 -e "CREATE TABLE t(a INT) ENGINE=NDB; INSERT INTO t VALUES(1); SELECT * FROM t; DROP TABLE t;"` — works end-to-end via proxy
- **Phase 2**: Same commands but SELECT returns via RonSQL (verify with debug logging)
- **Phase 3**: `USE test; SET NAMES utf8mb4; SELECT ...` works correctly
- **Phase 4**: `./mtr --suite=ronsql-mysql` passes; selected `ndb_*` tests pass through RonSQL port
- **Phase 5**: Connect with various MySQL clients (Python, Java, Go); TLS works
