# NDB Block Unit Tests

Unit tests that send raw signals to NDB kernel blocks using the SignalSender API.
These tests bypass the SQL layer and NDB API, directly exercising kernel block signal handlers.

## Tests

### testJoinAgg
Tests DBLQH pushdown join aggregation signal flow.

**Build:** `make -j$(sysctl -n hw.ncpu) testJoinAgg` (from debug_build)

**Run:**
```bash
testJoinAgg -c <connect_string> -m <mysql_port> [--verbose]
```

**Options:**
- `-c` — NDB management server connect string (default: localhost:1186)
- `-m` — MySQL server port for ndbinfo queries (default: 3306)
- `-v, --verbose` — Show detailed progress output
- `-h, --help` — Show help

**Tests:**
1. `SELECT SUM(b) FROM t GROUP BY a` — group-by with SUM aggregation (SCAN_FRAGREQ)
2. `SELECT COUNT(*), SUM(b) FROM t` — non-group-by with COUNT and SUM (SCAN_FRAGREQ)
3. High-cardinality GROUP BY — 200 groups, MUTEX_FREE strategy, exercises merge/send batching
4. Eviction via ERROR_INSERT 5090 — forces max 3 groups, tests eviction path with
   interleaved TRANSID_AI during scan phase
5. LQHKEYREQ with join aggregation — key operations (not scans), tests JoinAggFlag
   in LqhKeyReq variable data, uses DUMP 2359/2360 to bypass tc-node check

**Requires:** Running NDB cluster via `mtr --start-and-exit ndb_setup_large`
(Tests 3-5 need the large config with NodeId=144 for data node 2)

### testJoinAggNdbApi
Integration test for pushdown join aggregation using the NdbQueryBuilder API.
Tests the full NDB API path: NdbQueryBuilder → NdbQueryDef → NdbQuery → getAggregator().

**Build:** `make -j$(sysctl -n hw.ncpu) testJoinAggNdbApi` (from debug_build)

**Run:**
```bash
testJoinAggNdbApi -c <connect_string> -m <mysql_port> [--verbose]
```

**Options:**
- `-c` — NDB management server connect string (default: localhost:1186)
- `-m` — MySQL server port for table creation and verification (default: 3306)
- `-v, --verbose` — Show detailed progress output
- `-h, --help` — Show help

**Tests:**
1. `SELECT grp, SUM(amount) FROM parent JOIN child GROUP BY grp` — GROUP BY with SUM via pushed join
2. `SELECT COUNT(*), SUM(amount) FROM parent JOIN child` — non-GROUP-BY with COUNT and SUM
3. `SELECT grp, COUNT(*), SUM(amount) FROM parent JOIN child GROUP BY grp` — multiple aggregates with GROUP BY
4. `SELECT r.area, o.discount, SUM(l.amount), COUNT(*) FROM region JOIN order JOIN line ...` — 3-way join
   with linked parameter filter (`o.priority >= r.area`), multi-level linked attributes
   (grandparent + parent GROUP BY columns), wire format documented in comments

Each test first verifies expected results via MySQL JOIN query, then runs the
same query via NdbQueryBuilder with pushdown aggregation and compares results.
Tables are created and dropped via MySQL (ENGINE=NDB).

**Requires:** Running NDB cluster with MySQL server

### bench_q9_dbtc
TPC-H Q9 benchmark using raw DBTC→DBSPJ→DBLQH signals. 6-table join
(lineitem→part→orders→supplier→partsupp→nation) with composite keys,
multi-level linked attributes, and SUM aggregation with GROUP BY.

**Build:** `make -j$(sysctl -n hw.ncpu) bench_q9_dbtc`
**Requires:** TPC-H data loaded via `load_tpch`

### load_tpch
TPC-H data loader. Creates tables and populates with test data for
bench_q9_dbtc.

**Build:** `make -j$(sysctl -n hw.ncpu) load_tpch`
**Run:** `load_tpch -c <connect_string> -m <mysql_port>`

## Documentation
- `TESTING_GUIDE.md` — Detailed guide for writing block-level signal tests
  (SignalSender API, V_QUERY routing, AttrInfo construction, common pitfalls)

## Key Patterns
- NDB objects (Ndb, Ndb_cluster_connection, SignalSender) must be destroyed
  before calling ndb_end() — use scoping blocks
- Use V_QUERY with LDM instance number (not DBLQH instance 0) for scan signals
- SignalSender requires lock()/unlock() around all signal operations
