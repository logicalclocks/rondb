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
1. `SELECT SUM(b) FROM t GROUP BY a` — group-by with SUM aggregation
2. `SELECT COUNT(*), SUM(b) FROM t` — non-group-by with COUNT and SUM

**Requires:** Running NDB cluster (e.g. via `mtr --start ndb_basic`)

## Documentation
- `TESTING_GUIDE.md` — Detailed guide for writing block-level signal tests
  (SignalSender API, V_QUERY routing, AttrInfo construction, common pitfalls)

## Key Patterns
- NDB objects (Ndb, Ndb_cluster_connection, SignalSender) must be destroyed
  before calling ndb_end() — use scoping blocks
- Use V_QUERY with LDM instance number (not DBLQH instance 0) for scan signals
- SignalSender requires lock()/unlock() around all signal operations
