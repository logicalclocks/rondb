# DBSPJ Block Documentation

DBSPJ (DataBase Select-Project-Join) handles pushed-down join execution in RonDB/NDB Cluster.

## AttributeInformation Section

The AttributeInformation section of LQHKEYREQ and SCAN_FRAGREQ signals contains virtual programs (interpreted code) used for join processing. Each table in a join has two logical sections for interpreted code execution.

### Message Reception

| Signal | Handler | Section Number |
|--------|---------|----------------|
| LQHKEYREQ (lookups) | `execLQHKEYREQ()` | `LqhKeyReq::AttrInfoSectionNum = 1` |
| SCAN_FRAGREQ (scans) | `execSCAN_FRAGREQ()` | `ScanFragReq::AttrInfoSectionNum = 1` |

Section 0 contains KeyInfo, Section 1 contains AttributeInformation.

### 5-Word Header Structure

The section starts with a 5-word header (DbspjMain.cpp:9392):

| Index | Purpose | Description |
|-------|---------|-------------|
| 0 | Reserved | Unused |
| 1 | Interpreted Program Length | Size of filter/predicate bytecode |
| 2 | Reserved | Unused |
| 3 | Final Read Length | Size of attribute read list |
| 4 | Subroutine Length | Interpreter subroutine code (rarely used) |

### Two Sections Per Table

Each table participating in the join has two logical sections:

**Section 1: Interpreted Program (Filtering)**
- Purpose: Execute filter logic BEFORE expensive column reads
- Size Field: `sectionptrs[1]`
- Content: Interpreter bytecode (READ_ATTR_INTO_REG, BRANCH_*, EXIT_OK, EXIT_REFUSE)
- Execution: FIRST by Dbtup
- Default: Single word `Interpreter::ExitOK()` if no filter specified

**Section 2: Final Read Attributes (Column Reads)**
- Purpose: Read only necessary columns AFTER filter acceptance
- Size Field: `sectionptrs[3]`
- Content: AttributeHeader list for columns to retrieve
- Execution: LAST by Dbtup (only if interpreter returns EXIT_OK)

### Complete Section Content Order

After the 5-word header:

1. **Interpreted Program** (`sectionptrs[1]` words)
   - Compiled filter bytecode from `NI_ATTR_INTERPRET` and `PI_ATTR_INTERPRET` bits

2. **User Projection** (PI_ATTR_LIST content)
   - Attribute headers for user-requested columns

3. **Linked Attributes** (NI_LINKED_ATTR list)
   - Attribute headers for columns passed to child operations
   - Includes CORR_FACTOR32 for row correlation tracking

4. **FLUSH_AI Marker** (optional, 4 words)
   - Routes user projection results to API client
   ```cpp
   flush[0] = AttributeHeader::FLUSH_AI << 16;
   flush[1] = ctx.m_resultRef;      // API reference
   flush[2] = ctx.m_resultData;     // API data
   flush[3] = ctx.m_senderRef;      // Route reference
   ```

5. **Final Read Section** (`sectionptrs[3]` words)
   - AttributeHeader entries for post-filter column reads

### DABits Flags (QueryTree.hpp)

**Node Info Bits** (in QueryNode requestInfo):
```cpp
NI_HAS_PARENT     = 0x01    // Node has parent dependency
NI_KEY_LINKED     = 0x02    // Key contains linked values from parent
NI_KEY_PARAMS     = 0x04    // Key contains parameters
NI_KEY_CONSTS     = 0x08    // Key contains constants
NI_LINKED_ATTR    = 0x10    // Attributes passed to children
NI_ATTR_INTERPRET = 0x20    // Interpreter code in tree (rare)
NI_ATTR_LINKED    = 0x80    // Attributes contain linked values
NI_INNER_JOIN     = 0x400   // Inner join semantics
NI_FIRST_MATCH    = 0x800   // Return only first match
NI_ANTI_JOIN      = 0x1000  // Anti-join semantics
```

**Parameter Info Bits** (in QueryNodeParameters requestInfo):
```cpp
PI_ATTR_LIST      = 0x1     // User projection list
PI_KEY_PARAMS     = 0x4     // Key parameters
PI_ATTR_INTERPRET = 0x8     // Interpreter code in parameters
PI_DISK_ATTR      = 0x10    // Disk column in projection
```

### Query Pattern Types

Used for dynamic attribute construction (from parent row values):
```cpp
P_DATA         = 0x1    // Raw constant data
P_COL          = 0x2    // Column value from row
P_UNQ_PK       = 0x3    // PK from unique index
P_PARAM        = 0x4    // User parameter
P_PARENT       = 0x5    // Move up in tree (prefix to P_COL)
P_PARAM_HEADER = 0x6    // Parameter with AttributeHeader
P_ATTRINFO     = 0x7    // Column with header
```

### Execution Flow

```
API sends LQHKEYREQ/SCAN_FRAGREQ
              |
              v
SPJ parseDA() builds attrInfo:
  1. Add 5-word section header
  2. Add interpreted program (sectionptrs[1])
  3. Add user projections (if PI_ATTR_LIST)
  4. Add FLUSH_AI marker (if needed)
  5. Add linked attributes (if NI_LINKED_ATTR)
  6. Store final read length in sectionptrs[3]
              |
              v
SPJ sends sections to LQH/Dbtup
              |
              v
Dbtup receives attrInfo:
  1. Read 5-word header
  2. Execute interpreter program
     - Returns EXIT_OK or EXIT_REFUSE
  3. If EXIT_OK -> Execute final reads
  4. If EXIT_REFUSE -> Skip row
              |
              v
LQH sends TRANSID_AI to SPJ with results
              |
              v
SPJ routes to children or API
```

### Example: Filtered Join

**Query:** `SELECT * FROM t1, t2 WHERE t1.id = t2.parent_id AND t1.status > 5`

**TreeNode 1 (t1 - root scan):**
```
Flags: NI_ATTR_INTERPRET=1, NI_LINKED_ATTR=1, PI_ATTR_LIST=1

attrInfo:
[0-4]: Header [0, prog_len, 0, read_len, 0]
[5-n]: Interpreted program:
       - READ_ATTR_INTO_REG(status, reg0)
       - LOAD_CONST(5, reg1)
       - BRANCH_GT_REG_REG(reg0, reg1, exit_ok)
       - EXIT_REFUSE
       - EXIT_OK
[n+1-m]: User projection (all t1 columns)
[m+1-p]: Linked attributes (t1.id + CORR_FACTOR32)
```

**TreeNode 2 (t2 - child lookup):**
```
Flags: NI_KEY_LINKED=1, NI_ATTR_LINKED=1

attrInfo:
[0-4]: Header [0, 1, 0, read_len, 0]
[5]:   Interpreter::ExitOK()
[6+]:  Final reads (t2 columns)
```

### Key Data Structures

**TreeNode** (Dbspj.hpp):
```cpp
struct TreeNode {
    Uint32 m_bits;                        // T_ATTR_INTERPRETED flag
    Uint32 m_send.m_attrInfoPtrI;         // Section pointer
    PatternStore::Head m_attrParamPattern; // Runtime expansion pattern
    Uint32 m_tableOrIndexId;
};
```

**TreeNode Bits**:
```cpp
T_ATTR_INTERPRETED     = 0x1      // Has interpreter code
T_KEYINFO_CONSTRUCTED  = 0x4      // Key rebuilt per send
T_ATTRINFO_CONSTRUCTED = 0x8      // Attr rebuilt per send
T_USER_PROJECTION      = 0x20     // Has user result projection
T_INNER_JOIN           = 0x40000  // Inner join enabled
T_FIRST_MATCH          = 0x200000 // First match only
```

### Key Functions

| Function | Location | Purpose |
|----------|----------|---------|
| `parseDA()` | DbspjMain.cpp:9182 | Parses query tree and builds attrInfo sections |
| `expand()` | DbspjMain.cpp:9025, 9095 | Processes QueryPattern elements |
| `lookup_send()` | DbspjMain.cpp:4665 | Sends LQHKEYREQ with prepared attrInfo |
| `execTRANSID_AI()` | DbspjMain.cpp:3486 | Receives results, routes to children/API |

### Design Benefits

This design enables **predicate pushdown** where filtering happens at data nodes before column retrieval, significantly reducing network traffic for joins with selective filters.

---

## Aggregate Pushdown Design (Implemented)

This section documents the DBSPJ protocol changes for pushed-down aggregate queries. DBSPJ acts as the controller; the actual aggregation execution happens in DBLQH/Dbtup and is treated as a black box.

### Design Principle

- **DBSPJ role**: Route the aggregation program to the last table, ensure only the last table sends results to API
- **DBLQH role**: Execute the aggregation interpreter program (black box to DBSPJ)
- **Key change**: Intermediate tables pass data via linked attributes; only the leaf table sends to API

### Current vs Aggregate Flow

**Current (no aggregation):**
```
Table 1 (root)     --FLUSH_AI--> API (user projection)
    |
    +--linked attrs--> Table 2 --FLUSH_AI--> API
                           |
                           +--linked attrs--> Table 3 --FLUSH_AI--> API
```

**With Aggregation:**
```
Table 1 (root)     --NO FLUSH_AI-- (intermediate, no API send)
    |
    +--linked attrs--> Table 2 --NO FLUSH_AI-- (intermediate)
                           |
                           +--linked attrs + agg columns--> Table 3 (leaf)
                                                               |
                                                               v
                                                    DBLQH executes aggregation
                                                               |
                                                               v
                                                         FLUSH_AI --> API
                                                    (aggregated results only)
```

### Protocol Extensions

#### New DABits Flags (QueryTree.hpp)

**Node Info Bits:**
```cpp
NI_AGGREGATE      = 0x2000  // Request contains aggregation, only leaf sends to API
NI_AGGREGATE_LEAF = 0x4000  // This node executes aggregation and sends to API
```

**Parameter Info Bits:**
```cpp
PI_ATTR_AGGREGATE = 0x20    // Aggregation program in parameters (for DBLQH)
```

#### New TreeNode Bit (Dbspj.hpp)
```cpp
T_AGGREGATE_LEAF = 0x1000000  // Node executes aggregation, sends to API
```

#### New Request Bit (Dbspj.hpp)
```cpp
RT_AGGREGATE = 0x80  // Request contains aggregation (only leaf sends to API)
```

### AttributeInformation Changes

#### For Intermediate Tables (Non-Leaf)

When `NI_AGGREGATE` is set but `NI_AGGREGATE_LEAF` is not:

1. **No FLUSH_AI marker** - intermediate tables don't send to API
2. **No PI_ATTR_LIST user projection** - no direct results to API
3. **Extended NI_LINKED_ATTR** - pass all columns needed for:
   - Join key columns for child lookups
   - Aggregation source columns (e.g., the column being SUMmed)
   - GROUP BY columns
   - CORR_FACTOR32 for correlation

```
attrInfo for intermediate table:
[0-4]: Header [0, prog_len, 0, read_len, 0]
[5-n]: Interpreted program (filter only, no aggregation)
[n+1-m]: Linked attributes (join keys + agg columns + GROUP BY + CORR_FACTOR32)
         ** NO FLUSH_AI **
[m+1-p]: Final reads
```

#### For Leaf Table (Aggregation Node)

When both `NI_AGGREGATE` and `NI_AGGREGATE_LEAF` are set:

1. **Aggregation program included** - from `PI_ATTR_AGGREGATE`
2. **FLUSH_AI marker present** - only this table sends to API
3. **User projection contains aggregate results** - SUM, COUNT, etc.

```
attrInfo for leaf (aggregation) table:
[0-4]: Header [0, prog_len, 0, read_len, 0]
[5-n]: Interpreted program (filter + aggregation logic for DBLQH)
[n+1-m]: User projection (aggregate result columns)
[m+1-p]: FLUSH_AI marker (routes aggregated results to API)
[p+1-q]: Final reads (columns needed for aggregation)
```

### parseDA() Implementation

#### Setting Aggregate Bits (DbspjMain.cpp:9236)

```cpp
if (treeBits & DABits::NI_AGGREGATE) {
  jam();
  DEBUG("AGGREGATE: request contains aggregation");
  requestPtr.p->m_bits |= Request::RT_AGGREGATE;

  if (treeBits & DABits::NI_AGGREGATE_LEAF) {
    jam();
    DEBUG("AGGREGATE_LEAF: this node sends aggregated results to API");
    treeNodePtr.p->m_bits |= TreeNode::T_AGGREGATE_LEAF;
  }
}  // DABits::NI_AGGREGATE
```

#### Suppressing FLUSH_AI for Intermediate Tables (DbspjMain.cpp:9619)

```cpp
const bool isAggregateRequest =
    (requestPtr.p->m_bits & Request::RT_AGGREGATE) != 0;
const bool isAggregateLeaf =
    (treeNodePtr.p->m_bits & TreeNode::T_AGGREGATE_LEAF) != 0;
const bool suppressFlushAI = isAggregateRequest && !isAggregateLeaf;

if (!suppressFlushAI &&
    (treeBits & DABits::NI_LINKED_ATTR || requestPtr.p->isScan() ||
     !ndbd_spj_api_support_short_TRANSID_AI(API_version))) {
  // Insert FLUSH_AI marker
  jam();
  Uint32 flush[4];
  flush[0] = AttributeHeader::FLUSH_AI << 16;
  flush[1] = ctx.m_resultRef;
  flush[2] = ctx.m_resultData;
  flush[3] = ctx.m_senderRef;  // RouteRef
  appendToSection(attrInfoPtrI, flush, 4);
}
```

### Linked Attributes Extension

For aggregation queries, linked attributes must include additional columns:

| Column Type | Purpose | Example |
|-------------|---------|---------|
| Join key | Child table lookup | `t1.id` for `t2.parent_id = t1.id` |
| Aggregate source | Column to aggregate | `t2.amount` for `SUM(t2.amount)` |
| GROUP BY | Grouping key | `t1.category` for `GROUP BY t1.category` |
| CORR_FACTOR32 | Row correlation | Always included |

The query builder (outside DBSPJ) ensures all necessary columns appear in `NI_LINKED_ATTR` for intermediate tables.

### Result Handling

#### execTRANSID_AI Changes

Minimal changes needed - DBLQH sends aggregated results via TRANSID_AI:

```cpp
void Dbspj::execTRANSID_AI(Signal *signal) {
    // ... existing processing ...

    // For aggregate leaf: results are already aggregated by DBLQH
    // Just route to API as normal (FLUSH_AI handles routing)

    // For intermediate aggregate nodes: no TRANSID_AI expected
    // (no T_EXPECT_TRANSID_AI bit set, or rows just passed through)
}
```

#### sendConf Changes

Row count interpretation changes for aggregation:

```cpp
void Dbspj::sendConf(Signal *signal, Ptr<Request> requestPtr, bool is_complete) {
    ScanFragConf *conf = ...;

    // m_rows now contains aggregate result count, not source row count
    // (DBLQH returns one row per GROUP, not one per source row)
    conf->completedOps = requestPtr.p->m_rows;
}
```

### Example: Aggregate Join Query

**Query:** `SELECT t1.category, SUM(t2.amount) FROM t1 JOIN t2 ON t1.id = t2.parent_id GROUP BY t1.category`

**TreeNode 1 (t1 - root scan, intermediate):**
```
Flags: NI_AGGREGATE=1, NI_LINKED_ATTR=1
       (NOT NI_AGGREGATE_LEAF, NOT PI_ATTR_LIST)

attrInfo:
[0-4]: Header [0, 1, 0, read_len, 0]
[5]:   Interpreter::ExitOK() (no filter)
[6+]:  Linked attributes:
       - t1.id (join key for child)
       - t1.category (GROUP BY column, passed to leaf)
       - CORR_FACTOR32

** NO FLUSH_AI - intermediate table **
```

**TreeNode 2 (t2 - leaf with aggregation):**
```
Flags: NI_AGGREGATE=1, NI_AGGREGATE_LEAF=1, NI_KEY_LINKED=1,
       PI_ATTR_LIST=1, PI_ATTR_AGGREGATE=1

attrInfo:
[0-4]: Header [0, agg_prog_len, 0, read_len, 0]
[5-n]: Aggregation interpreter program (black box from PI_ATTR_AGGREGATE)
       - GROUP BY logic
       - SUM accumulation
       - Handled entirely by DBLQH
[n+1-m]: User projection:
       - AttributeHeader for category (from parent linked attr)
       - AttributeHeader for SUM result
[m+1-p]: FLUSH_AI marker (routes to API)
[p+1-q]: Final reads (t2.amount for SUM input)
```

**Execution Flow:**
```
1. Root scan reads t1 rows
2. Linked attrs (id, category) passed to child via CORR_FACTOR32
3. Child lookup finds t2 rows, DBLQH executes aggregation program
4. DBLQH accumulates SUM(amount) per category group
5. Aggregated results (category, sum) sent via TRANSID_AI
6. FLUSH_AI routes to API
7. ScanFragConf reports group count (not source row count)
```

---

### AttributeInformation Format for Aggregation Queries

This section provides detailed formatting rules for the AttributeInformation section when aggregation is pushed down through a join.

#### General Principles

1. **Intermediate nodes** (have `NI_AGGREGATE` but NOT `NI_AGGREGATE_LEAF`):
   - No `FLUSH_AI` marker - results do not go to API
   - No user projection (`PI_ATTR_LIST`) for API consumption
   - Extended linked attributes to pass all required columns downstream:
     - Join keys for child table lookups
     - GROUP BY columns (must reach the leaf)
     - Columns needed for aggregate functions (e.g., column being SUMmed)
   - Standard interpreted program for filtering (if any)

2. **Leaf node** (has both `NI_AGGREGATE` and `NI_AGGREGATE_LEAF`):
   - Contains the aggregation program (`PI_ATTR_AGGREGATE`) - black box for DBLQH
   - Has `FLUSH_AI` marker - only node sending to API
   - User projection contains aggregate result columns
   - Final reads include columns needed as input to aggregation

3. **Column flow**: GROUP BY columns and aggregate source columns must flow from their origin table through all intermediate tables via linked attributes until they reach the leaf.

#### Intermediate Node AttributeInformation Structure

```
+------------------+----------------------------------------+
| Offset           | Content                                |
+------------------+----------------------------------------+
| [0]              | Reserved (0)                           |
| [1]              | sectionptrs[1]: Interpreted prog len   |
| [2]              | Reserved (0)                           |
| [3]              | sectionptrs[3]: Final read len         |
| [4]              | Reserved (0)                           |
+------------------+----------------------------------------+
| [5..n]           | Interpreted program (filter only)      |
|                  | - Predicate evaluation                 |
|                  | - Ends with EXIT_OK or EXIT_REFUSE     |
|                  | - Default: single word ExitOK()        |
+------------------+----------------------------------------+
| [n+1..m]         | Linked attributes (NI_LINKED_ATTR):    |
|                  | - Join key columns for child lookup    |
|                  | - GROUP BY columns (pass-through)      |
|                  | - Aggregate source columns             |
|                  | - CORR_FACTOR32 (always last)          |
+------------------+----------------------------------------+
|                  | ** NO FLUSH_AI **                      |
+------------------+----------------------------------------+
| [m+1..p]         | Final read attributes                  |
|                  | (columns to retrieve after filter)     |
+------------------+----------------------------------------+
```

#### Leaf Node (Aggregation) AttributeInformation Structure

```
+------------------+----------------------------------------+
| Offset           | Content                                |
+------------------+----------------------------------------+
| [0]              | Reserved (0)                           |
| [1]              | sectionptrs[1]: Aggregation prog len   |
| [2]              | Reserved (0)                           |
| [3]              | sectionptrs[3]: Final read len         |
| [4]              | Reserved (0)                           |
+------------------+----------------------------------------+
| [5..n]           | Aggregation program (PI_ATTR_AGGREGATE)|
|                  | - Black box executed by DBLQH          |
|                  | - Contains GROUP BY logic              |
|                  | - Contains aggregate accumulation      |
|                  | - Format defined by DBLQH interpreter  |
+------------------+----------------------------------------+
| [n+1..m]         | User projection (PI_ATTR_LIST):        |
|                  | - GROUP BY result columns              |
|                  | - Aggregate result columns (SUM, etc.) |
+------------------+----------------------------------------+
| [m+1..m+4]       | FLUSH_AI marker:                       |
|                  | [0] AttributeHeader::FLUSH_AI << 16    |
|                  | [1] ctx.m_resultRef (API reference)    |
|                  | [2] ctx.m_resultData (API data)        |
|                  | [3] ctx.m_senderRef (RouteRef)         |
+------------------+----------------------------------------+
| [m+5..p]         | Linked attributes (if any children)    |
|                  | + CORR_FACTOR32                        |
+------------------+----------------------------------------+
| [p+1..q]         | Final read attributes                  |
|                  | - Columns needed for aggregation input |
+------------------+----------------------------------------+
```

---

### TPC-H Three-Way Join Aggregation Example

This section provides a detailed example using TPC-H schema with exact hexadecimal values for the AttributeInformation section.

#### Key Constants

```cpp
// From AttributeHeader.hpp
FLUSH_AI      = 0xFFEA    // Pseudo-attribute: flush results to API
CORR_FACTOR32 = 0xFFE9    // Pseudo-attribute: 32-bit correlation ID

// From Interpreter.hpp
EXIT_OK = 18              // Instruction: accept row (0x00000012)

// AttributeHeader format: (attrId << 16) | byteSize
// For linked attrs (size=0): attrId << 16
```

#### TPC-H Schema (Relevant Columns)

```
CUSTOMER table (tableId = 1):
  Column 0: C_CUSTKEY     - INT (4 bytes)     - Primary key
  Column 6: C_MKTSEGMENT  - CHAR(10) (10 bytes)

ORDERS table (tableId = 2):
  Column 0: O_ORDERKEY    - INT (4 bytes)     - Primary key
  Column 1: O_CUSTKEY     - INT (4 bytes)     - Foreign key to CUSTOMER

LINEITEM table (tableId = 3):
  Column 0: L_ORDERKEY    - INT (4 bytes)     - Part of primary key
  Column 5: L_EXTENDEDPRICE - DECIMAL (8 bytes)
```

#### Query

```sql
-- TPC-H inspired: Revenue by market segment
SELECT
    C_MKTSEGMENT,
    COUNT(*),
    SUM(L_EXTENDEDPRICE)
FROM CUSTOMER, ORDERS, LINEITEM
WHERE C_CUSTKEY = O_CUSTKEY
  AND O_ORDERKEY = L_ORDERKEY
GROUP BY C_MKTSEGMENT
```

#### Join Tree Structure

```
TreeNode 0: CUSTOMER (root scan)     - Intermediate
    |
    +-- TreeNode 1: ORDERS (lookup)  - Intermediate
            |
            +-- TreeNode 2: LINEITEM (lookup) - Aggregation Leaf
```

---

#### TreeNode 0: CUSTOMER (Root Scan - Intermediate)

**DABits flags:**
```
treeBits  = 0x2010  (NI_AGGREGATE=0x2000 | NI_LINKED_ATTR=0x10)
paramBits = 0x0000  (no PI_ATTR_LIST - intermediate node)
```

**AttributeInformation - 9 words total:**

```
Word  Hex Value    Decimal    Description
----  ----------   --------   ------------------------------------------
[0]   0x00000000   0          Reserved
[1]   0x00000001   1          Interpreted program length: 1 word
[2]   0x00000000   0          Reserved
[3]   0x00000002   2          Final read length: 2 words
[4]   0x00000000   0          Reserved
----  ----------   --------   ------------------------------------------
                              INTERPRETED PROGRAM (1 word)
[5]   0x00000012   18         Interpreter::EXIT_OK - accept all rows
----  ----------   --------   ------------------------------------------
                              LINKED ATTRIBUTES (3 words)
[6]   0x00000000   0          C_CUSTKEY (col 0) << 16 - join key for ORDERS
[7]   0x00060000   393216     C_MKTSEGMENT (col 6) << 16 - GROUP BY column
[8]   0xFFE90000   4293591040 CORR_FACTOR32 << 16 - correlation ID
----  ----------   --------   ------------------------------------------
                              ** NO FLUSH_AI ** (intermediate node)
----  ----------   --------   ------------------------------------------
                              FINAL READ ATTRIBUTES (2 words)
[9]   0x00000004   4          C_CUSTKEY: (col 0 << 16) | 4 bytes
[10]  0x0006000A   393226     C_MKTSEGMENT: (col 6 << 16) | 10 bytes
----  ----------   --------   ------------------------------------------
```

**Complete section as hex dump:**
```
Offset  +0         +4         +8         +C
0x00:   00000000   00000001   00000000   00000002
0x10:   00000000   00000012   00000000   00060000
0x20:   FFE90000   00000004   0006000A
```

**Key points:**
- No FLUSH_AI because RT_AGGREGATE is set but T_AGGREGATE_LEAF is not
- C_MKTSEGMENT (col 6) passed via linked attrs for GROUP BY at leaf
- C_CUSTKEY (col 0) is join key for ORDERS lookup

---

#### TreeNode 1: ORDERS (Lookup - Intermediate)

**DABits flags:**
```
treeBits  = 0x2013  (NI_AGGREGATE=0x2000 | NI_LINKED_ATTR=0x10 |
                     NI_KEY_LINKED=0x02 | NI_HAS_PARENT=0x01)
paramBits = 0x0000  (no PI_ATTR_LIST - intermediate node)
```

**AttributeInformation - 12 words total:**

```
Word  Hex Value    Decimal    Description
----  ----------   --------   ------------------------------------------
[0]   0x00000000   0          Reserved
[1]   0x00000001   1          Interpreted program length: 1 word
[2]   0x00000000   0          Reserved
[3]   0x00000002   2          Final read length: 2 words
[4]   0x00000000   0          Reserved
----  ----------   --------   ------------------------------------------
                              INTERPRETED PROGRAM (1 word)
[5]   0x00000012   18         Interpreter::EXIT_OK - accept all rows
----  ----------   --------   ------------------------------------------
                              LINKED ATTRIBUTES (4 words)
[6]   0x00000000   0          O_ORDERKEY (col 0) << 16 - join key for LINEITEM
[7]   0x00060000   393216     C_MKTSEGMENT (col 6 from parent) << 16 - pass-through
[8]   0x00050000   327680     L_EXTENDEDPRICE placeholder << 16 - for leaf access
[9]   0xFFE90000   4293591040 CORR_FACTOR32 << 16 - correlation ID
----  ----------   --------   ------------------------------------------
                              ** NO FLUSH_AI ** (intermediate node)
----  ----------   --------   ------------------------------------------
                              FINAL READ ATTRIBUTES (2 words)
[10]  0x00000004   4          O_ORDERKEY: (col 0 << 16) | 4 bytes
[11]  0x00010004   65540      O_CUSTKEY: (col 1 << 16) | 4 bytes
----  ----------   --------   ------------------------------------------
```

**Complete section as hex dump:**
```
Offset  +0         +4         +8         +C
0x00:   00000000   00000001   00000000   00000002
0x10:   00000000   00000012   00000000   00060000
0x20:   00050000   FFE90000   00000004   00010004
```

**Key points:**
- No FLUSH_AI because this is an intermediate aggregate node
- C_MKTSEGMENT passed through from CUSTOMER for GROUP BY at leaf
- O_ORDERKEY is join key for LINEITEM lookup

---

#### TreeNode 2: LINEITEM (Lookup - Aggregation Leaf)

**DABits flags:**
```
treeBits  = 0x6003  (NI_AGGREGATE=0x2000 | NI_AGGREGATE_LEAF=0x4000 |
                     NI_KEY_LINKED=0x02 | NI_HAS_PARENT=0x01)
paramBits = 0x0021  (PI_ATTR_LIST=0x01 | PI_ATTR_AGGREGATE=0x20)
```

**AttributeInformation - variable length, example with 10-word aggregation program:**

Assume aggregation program is 10 words (provided by query builder, executed by DBLQH).

```
Word  Hex Value    Decimal    Description
----  ----------   --------   ------------------------------------------
[0]   0x00000000   0          Reserved
[1]   0x0000000A   10         Interpreted program length: 10 words (agg program)
[2]   0x00000000   0          Reserved
[3]   0x00000002   2          Final read length: 2 words
[4]   0x00000000   0          Reserved
----  ----------   --------   ------------------------------------------
                              AGGREGATION PROGRAM (10 words) - BLACK BOX
[5]   0x????????   ?          Aggregation instruction 1
[6]   0x????????   ?            - GROUP BY setup
[7]   0x????????   ?            - Grouping key: C_MKTSEGMENT
[8]   0x????????   ?            - COUNT accumulator init
[9]   0x????????   ?            - SUM accumulator init
[10]  0x????????   ?            - Accumulation logic
[11]  0x????????   ?            - ...
[12]  0x????????   ?            - ...
[13]  0x????????   ?            - Result finalization
[14]  0x????????   ?            - Program end
----  ----------   --------   ------------------------------------------
                              USER PROJECTION (3 words) - aggregate results
[15]  0x0006000A   393226     C_MKTSEGMENT result: (col 6 << 16) | 10 bytes
[16]  0x00800004   8388612    COUNT(*) result: (pseudo-col 128 << 16) | 4 bytes
[17]  0x00810008   8454152    SUM result: (pseudo-col 129 << 16) | 8 bytes
----  ----------   --------   ------------------------------------------
                              FLUSH_AI MARKER (4 words)
[18]  0xFFEA0000   4293459968 AttributeHeader::FLUSH_AI << 16
[19]  0x????????   ?          ctx.m_resultRef - API block reference
[20]  0x????????   ?          ctx.m_resultData - API operation data
[21]  0x????????   ?          ctx.m_senderRef - Route reference (TC block)
----  ----------   --------   ------------------------------------------
                              (no linked attrs - this is a leaf node)
----  ----------   --------   ------------------------------------------
                              FINAL READ ATTRIBUTES (2 words)
[22]  0x00050008   327688     L_EXTENDEDPRICE: (col 5 << 16) | 8 bytes
[23]  0x00000004   4          L_ORDERKEY: (col 0 << 16) | 4 bytes
----  ----------   --------   ------------------------------------------
```

**Complete section as hex dump (with placeholders for variable content):**
```
Offset  +0         +4         +8         +C
0x00:   00000000   0000000A   00000000   00000002
0x10:   00000000   ????????   ????????   ????????
0x20:   ????????   ????????   ????????   ????????
0x30:   ????????   ????????   ????????   0006000A
0x40:   00800004   00810008   FFEA0000   ????????
0x50:   ????????   ????????   00050008   00000004
```

**Key points:**
- Has FLUSH_AI because T_AGGREGATE_LEAF is set - only node sending to API
- Aggregation program is a black box (10 words in this example) - DBSPJ passes through
- User projection contains aggregate result schema (GROUP BY col + aggregates)
- Final reads include L_EXTENDEDPRICE (SUM input) and L_ORDERKEY (join verification)

---

#### FLUSH_AI Marker Detail

The FLUSH_AI marker is exactly 4 words:

```
Word  Hex Value    Description
----  ----------   --------------------------------------------------
[0]   0xFFEA0000   (FLUSH_AI << 16) | 0 = (0xFFEA << 16)
[1]   0x????????   m_resultRef: Block reference of API client
                   Format: (nodeId << 16) | blockNo
                   Example: 0x00010FA0 = node 1, block 4000 (API)
[2]   0x????????   m_resultData: Operation identifier for API
                   Example: 0x00000001 = operation ID 1
[3]   0x????????   m_senderRef: Route reference (TC block)
                   Format: (nodeId << 16) | blockNo
                   Example: 0x000107D0 = node 1, block 2000 (DBTC)
```

---

#### Correlation ID Format

The CORR_FACTOR32 value returned by DBLQH encodes row relationships:

```
Bits 31-16: Parent tuple ID (identifies parent row in batch)
Bits 15-0:  Current tuple ID (identifies this row in batch)

Example correlations during execution:
CUSTOMER row 1:     0x00000001  (no parent, tuple 1)
  ORDERS row 1:     0x00010001  (parent=1, tuple 1)
  ORDERS row 2:     0x00010002  (parent=1, tuple 2)
    LINEITEM row 1: 0x00020001  (parent=2, tuple 1)
    LINEITEM row 2: 0x00020002  (parent=2, tuple 2)
```

---

#### Data Flow with Hex Values

```
CUSTOMER (scan)                 ORDERS (lookup)                LINEITEM (lookup + aggregate)
================               ================                ============================

Row: C_CUSTKEY=1, C_MKTSEGMENT="BUILDING"
Correlation: 0x00000001
     |
     +-- Linked attrs sent:
     |   [0]: 0x00000000 (C_CUSTKEY col 0)
     |   [1]: 0x00060000 (C_MKTSEGMENT col 6)
     |   [2]: 0xFFE90000 (CORR_FACTOR32)
     |
     +-------> Row: O_ORDERKEY=100, O_CUSTKEY=1
               Correlation: 0x00010001
                    |
                    +-- Linked attrs sent:
                    |   [0]: 0x00000000 (O_ORDERKEY col 0)
                    |   [1]: 0x00060000 (C_MKTSEGMENT passthrough)
                    |   [2]: 0xFFE90000 (CORR_FACTOR32)
                    |
                    +-------> Row: L_ORDERKEY=100, L_EXTENDEDPRICE=1500.00
                              Correlation: 0x00010001

                              DBLQH executes aggregation program:
                              - GROUP BY key: "BUILDING"
                              - COUNT++: 1
                              - SUM += 1500.00

                              (accumulates across all matching rows)

                              On batch complete, FLUSH_AI sends:
                              +------------------------------------------+
                              | C_MKTSEGMENT | COUNT(*) | SUM(price)     |
                              +------------------------------------------+
                              | "BUILDING"   | 47       | 125000.00      |
                              | "AUTOMOBILE" | 38       |  98500.00      |
                              | "MACHINERY"  | 29       |  67200.00      |
                              +------------------------------------------+
                                        |
                                        v
                                       API
```

---

#### Complete Byte Layout Summary

**TreeNode 0 (CUSTOMER) - 44 bytes:**
```
00000000 00000001 00000000 00000002 00000000
00000012 00000000 00060000 FFE90000 00000004
0006000A
```

**TreeNode 1 (ORDERS) - 48 bytes:**
```
00000000 00000001 00000000 00000002 00000000
00000012 00000000 00060000 00050000 FFE90000
00000004 00010004
```

**TreeNode 2 (LINEITEM) - 96 bytes (with 10-word agg program):**
```
00000000 0000000A 00000000 00000002 00000000
[10 words aggregation program - content from DBLQH]
0006000A 00800004 00810008 FFEA0000 [resultRef]
[resultData] [senderRef] 00050008 00000004
```

---

### Summary of DBSPJ-Only Changes

| Component | Change |
|-----------|--------|
| DABits (QueryTree.hpp) | Add `NI_AGGREGATE`, `NI_AGGREGATE_LEAF`, `PI_ATTR_AGGREGATE` |
| TreeNode bits (Dbspj.hpp) | Add `T_AGGREGATE_LEAF` |
| parseDA() | Suppress FLUSH_AI for intermediate aggregate nodes |
| parseDA() | Include aggregation program for leaf node (pass-through to DBLQH) |
| Linked attributes | Ensure GROUP BY and aggregate source columns included |
| Result counting | `m_rows` counts aggregate results, not source rows |

### What DBSPJ Does NOT Do

- Parse or interpret the aggregation program (DBLQH's job)
- Accumulate aggregate values (DBLQH's job)
- Understand GROUP BY semantics (DBLQH's job)
- Format aggregate results (DBLQH's job)

DBSPJ simply routes the aggregation program to the correct table and ensures proper result flow back to the API.
