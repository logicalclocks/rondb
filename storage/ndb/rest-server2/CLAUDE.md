# Claude Code Development Notes for rdrs2

## Memory Allocation (ArenaMalloc)

**ArenaMalloc does not call constructors.** When allocating objects with `amalloc->alloc<T>(n)`, the memory is allocated but constructors are not invoked. This has important implications:

- **Virtual functions will not work** - The vtable pointer is not initialized, so virtual dispatch fails/crashes
- **Member variables are uninitialized** - Must explicitly initialize all members after allocation

### Allocation Methods

| Method | Zeroes Memory | Use Case |
|--------|---------------|----------|
| `alloc<T>(n)` | No | When you'll initialize all fields manually |
| `calloc<T>(n)` | Yes | When you want zero-initialized memory (pointers = nullptr, ints = 0) |
| `alloc_exc<T>(n)` | No | Like alloc but throws on failure |

### Solutions for Uninitialized Memory

1. **Use `calloc<T>(n)`** for zero-initialization (preferred for structs with pointers):
   ```cpp
   m_key_ops = amalloc->calloc<DeleteKeyOperation>(numOps);
   // All pointer fields are nullptr, all integer fields are 0
   ```

2. Use placement new for objects needing constructor calls:
   ```cpp
   PKRRequest *req = new (&key_op->m_req) PKRRequest(&reqBuffer[i]);
   ```

3. Avoid virtual dispatch - use non-virtual methods or move data to base class

4. Initialize members explicitly in `init_batch_operations` or similar

Example from pk_batch_base_operation.cpp:
```cpp
key_op->m_ndbTransaction = nullptr;
key_op->m_blob_handles = nullptr;
PKRRequest *req = new (&key_op->m_req) PKRRequest(&reqBuffer[i]);
```

## Base Class Destructor Limitations

**Cannot call virtual methods from base class destructor.** When the base destructor runs, the derived class has already been destroyed, so virtual dispatch to derived methods causes undefined behavior.

Solution: Cleanup that requires derived class data must be done in derived class destructors.

```cpp
// Wrong - crashes with pure virtual call
BaseBatchOperations::~BaseBatchOperations() {
  for (Uint32 i = 0; i < m_numOperations; i++) {
    get_key_op(i)->m_req.resetReadColumns();  // get_key_op is pure virtual!
  }
}

// Correct - each derived class handles its own cleanup
BatchKeyOperations::~BatchKeyOperations() {
  if (!m_isSuccess) {
    for (Uint32 i = 0; i < m_numOperations; i++) {
      m_key_ops[i].m_req.resetReadColumns();
    }
  }
}
```

## Batch Operations Refactoring Pattern

When adding new batch operations (like batch update), follow this pattern:

### Class Hierarchy
```
BaseKeyOperation          - Single row operation data (pk_base_operation.hpp)
  KeyOperation            - Read-specific (adds nothing currently)
  DeleteKeyOperation      - Delete-specific (adds m_blobColumns, m_num_blob_columns)

BaseBatchOperations       - Batch operation logic (pk_batch_base_operation.hpp)
  BatchKeyOperations      - Read batch operations (pkr_operation.hpp)
  BatchDeleteOperations   - Delete batch operations (pkd_operation.hpp)
```

### Virtual Methods to Override
```cpp
class BaseBatchOperations {
  // Required overrides:
  virtual BaseKeyOperation* get_key_op(Uint32 i) = 0;
  virtual RS_Status allocate_key_ops(ArenaMalloc*, Uint32) = 0;

  // Optional overrides (with defaults):
  virtual bool supports_blobs() const { return false; }
  virtual bool supports_read_all_columns() const { return false; }
  virtual NdbTransaction::ExecType get_single_transaction_exec_type() const {
    return NdbTransaction::Commit;
  }
};
```

### Steps to Add New Batch Operation Type

1. Create `XXX_operation.hpp` with:
   - `struct XXXKeyOperation : public BaseKeyOperation` (if extra fields needed)
   - `class BatchXXXOperations : public BaseBatchOperations`

2. Create `XXX_operation.cpp` with:
   - Constructor/destructor
   - `allocate_key_ops()` - allocate type-specific array
   - `setup_XXX_operations()` - set up NDB operations (readTuple/deleteTuple/etc)
   - `perform_operation()` - orchestrate the flow

3. Create controller `batch_XXX_ctrl.hpp/cpp`

4. Update `CMakeLists.txt` with new source files

### Key Differences Between Read, Delete, and Write

| Aspect | Read | Delete | Write |
|--------|------|--------|-------|
| `supports_blobs()` | true | false | true |
| `supports_read_all_columns()` | true | false | false |
| `get_single_transaction_exec_type()` | NoCommit | Commit | Commit |
| NDB operation | `readTuple()` | `deleteTuple()` | `writeTuple()`/`updateTuple()`/`insertTuple()` |
| Blob handling | Per readColumn | Per table column | Per writeColumn |
| JSON parser method | `batch_parse()` | `batch_parse_delete()` | `batch_parse_write()` |
| URL suffix | `pk-read` | `pk-delete` | `pk-write`/`pk-update`/`pk-insert` |

### REST API URL Format

The `relative-url` in batch operation JSON must use the correct suffix:

**Batch Read** (endpoint: `/<version>/batch`, method: POST):
```json
{
  "operations": [
    {
      "method": "POST",
      "relative-url": "database/table/pk-read",
      "body": { "filters": [...], "readColumns": [...] }
    }
  ]
}
```

**Batch Delete** (endpoint: `/<version>/batchdelete`, method: DELETE):
```json
{
  "operations": [
    {
      "method": "POST",
      "relative-url": "database/table/pk-delete",
      "body": { "filters": [...] }
    }
  ]
}
```

**Batch Write** (endpoint: `/<version>/batchwrite`, method: POST):
```json
{
  "operations": [
    {
      "method": "POST",
      "relative-url": "database/table/pk-write",
      "body": { "filters": [...], "writeColumns": [...] }
    },
    {
      "method": "POST",
      "relative-url": "database/table/pk-update",
      "body": { "filters": [...], "writeColumns": [...] }
    },
    {
      "method": "POST",
      "relative-url": "database/table/pk-insert",
      "body": { "filters": [...], "writeColumns": [...] }
    }
  ]
}
```

The URL suffix is validated in `extract_db_and_table()` or `extract_db_table_and_write_op()` - using the wrong suffix returns an error.

## Write Operation Types

Batch write supports three operation types, determined by the URL suffix:

| URL Suffix | NDB Method | Behavior |
|------------|------------|----------|
| `pk-write` | `writeTuple()` | Insert if row doesn't exist, update if it does |
| `pk-update` | `updateTuple()` | Update only, fails if row doesn't exist (404) |
| `pk-insert` | `insertTuple()` | Insert only, fails if row already exists (409) |

### HTTP Status Codes

Defined in `rdrs_dal.h`:

| Code | Constant | Usage |
|------|----------|-------|
| 200 | `SUCCESS` | Operation succeeded |
| 400 | `CLIENT_ERROR` | Invalid request (bad filters, missing columns, etc.) |
| 401 | `AUTH_ERROR` | Authentication failed |
| 404 | `NOT_FOUND` | Row not found (read/update/delete non-existent) |
| 409 | `CONFLICT` | Constraint violation (duplicate key on insert) |
| 500 | `SERVER_ERROR` | Internal server error |

**Error handling in batch operations** (`pk_batch_base_operation.cpp`):

```cpp
NdbError::Classification op_error = op->getNdbError().classification;
if (op_error == NdbError::NoError) {
  resp->SetStatus(SUCCESS, "OK");
} else if (op_error == NdbError::NoDataFound) {
  resp->SetStatus(NOT_FOUND, "NOT Found");
} else if (op_error == NdbError::ConstraintViolation) {
  resp->SetStatus(CONFLICT, op->getNdbError().message);
  return RS_RONDB_CONFLICT_ERROR(...);
} else {
  resp->SetStatus(SERVER_ERROR, op->getNdbError().message);
  return RS_RONDB_SERVER_ERROR(...);
}
```

## API Versioning

The REST API supports two versions:

| Version | Error Format | Description |
|---------|--------------|-------------|
| `0.1.0` | Plain text | Original version, errors returned as plain text |
| `0.2.0` | JSON | Errors returned as structured JSON objects |

Both versions are supported simultaneously - the version in the URL determines the error format.

### JSON Error Response Format (0.2.0)

API version 0.2.0 returns errors as JSON objects:

```json
{
  "error": {
    "code": 400,
    "message": "identifier is empty",
    "status": -1,
    "classification": -1,
    "ndbCode": -1,
    "mysqlCode": -1
  }
}
```

| Field | Description |
|-------|-------------|
| `code` | HTTP status code (400, 401, 404, 409, 500) |
| `message` | Human-readable error message |
| `status` | NdbError status (-1 if not from NDB) |
| `classification` | NdbError classification (-1 if not from NDB) |
| `ndbCode` | NdbError code (-1 if not from NDB) |
| `mysqlCode` | MySQL error code (-1 if not from NDB) |

### Error Response Helper (error_response.hpp)

The `error_response.hpp` header provides helpers for version-aware error responses:

```cpp
#include "error_response.hpp"

// Check if request uses JSON error version (0.2.0+)
if (isJsonErrorVersion(req)) {
  // Returns true for /0.2.0/* paths
}

// Set error response (auto-detects version)
setErrorResponse(req, resp, status);           // From RS_Status
setErrorResponse(req, resp, CLIENT_ERROR, "message");  // Simple message

// Build JSON error body manually
std::string json = buildJsonErrorBody(status);
std::string json = buildJsonErrorBody(CLIENT_ERROR, "message");
```

### Endpoint Registration Pattern

Controllers register handlers for both API versions:

```cpp
class BatchPKReadCtrl : public drogon::HttpController<BatchPKReadCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(BatchPKReadCtrl::batchPKRead, BATCH_PATH, drogon::Post);      // /0.1.0/batch
  ADD_METHOD_TO(BatchPKReadCtrl::batchPKRead, BATCH_PATH_V2, drogon::Post);   // /0.2.0/batch
  METHOD_LIST_END
  // Same handler for both versions - error format determined by isJsonErrorVersion()
};
```

### HTTP Methods by Endpoint

| Endpoint | HTTP Method | Notes |
|----------|-------------|-------|
| `/<version>/batch` | POST | Batch read operations |
| `/<version>/batchwrite` | POST | Batch write operations |
| `/<version>/batchdelete` | DELETE | Batch delete operations (RESTful) |
| `/<version>/ping` | GET | Health check |
| `/<version>/health` | GET | Detailed health status |
| `/<version>/{db}/{table}/pk-read` | POST | Single row read |
| `/<version>/ronsql` | POST | RonSQL queries |

### Implementation Details

**Operation type constants** (rdrs_const.h):
```cpp
#define RDRS_WRITE_OP_WRITE  0  // writeTuple
#define RDRS_WRITE_OP_UPDATE 1  // updateTuple
#define RDRS_WRITE_OP_INSERT 2  // insertTuple
```

**Flow**:
1. `batch_parse_write()` calls `extract_db_table_and_write_op()` to parse URL and determine operation type
2. Operation type stored in `PKReadParams.writeOperationType`
3. `create_native_write_request()` stores it in `PK_REQ_FLAGS_IDX`
4. `PKRRequest::WriteOperationType()` retrieves it from the request buffer
5. `setup_write_operations()` uses switch statement to call appropriate NDB method

**Key files**:
- `pkw_operation.hpp/cpp` - WriteKeyOperation struct and BatchWriteOperations class
- `json_parser.cpp` - `extract_db_table_and_write_op()` for URL parsing
- `encoding.cpp` - `create_native_write_request()` stores operation type

### Mixed Operations in Same Batch

A single batch can contain different operation types:
```json
{
  "operations": [
    {"relative-url": "db/t1/pk-write", ...},
    {"relative-url": "db/t2/pk-update", ...},
    {"relative-url": "db/t1/pk-insert", ...}
  ]
}
```

Each operation is processed independently with its own operation type.

## BLOB Handling in Write Operations

**Write operations support BLOB/TEXT columns via NdbBlob.** Unlike regular columns that are set in the row buffer, BLOB/TEXT columns must be written using `NdbBlob::setValue()`.

### How It Works

1. **Detection**: `setup_write_columns()` scans writeColumns for BLOB/TEXT types and sets `m_has_write_blobs = true`

2. **Single transaction mode**: When BLOBs are present, `m_single_transaction = true` is set (NdbBlob requires it)

3. **Row buffer setup**: In `setup_write_operations()`, BLOB/TEXT columns are **skipped** when setting values in the row buffer:
   ```cpp
   if (col->getType() == NdbDictionary::Column::Blob ||
       col->getType() == NdbDictionary::Column::Text) {
     continue;  // Skip - handled via NdbBlob
   }
   ```

4. **Blob handle activation**: After `writeTuple()`/`updateTuple()`/`insertTuple()`, get blob handles and set values:
   ```cpp
   NdbBlob *blobHandle = operation->getBlobHandle(col->getName());
   if (col->getType() == NdbDictionary::Column::Text) {
     blobHandle->setValue(valueCStr, valueLen);  // Direct text
   } else {
     // BLOB: base64 decode first
     base64_decode(valueCStr, valueLen, decodedBuffer, &decodedLen, 0);
     blobHandle->setValue(decodedBuffer, decodedLen);
   }
   ```

### Key Points

- **TEXT columns**: Value is passed directly as UTF-8 text
- **BLOB columns**: Value must be base64 encoded in the request; decoded before writing
- **Single transaction**: Required when any operation has BLOBs
- **WriteKeyOperation fields**:
  ```cpp
  const NdbDictionary::Column **m_writeColumns;  // Write column dictionary objects
  Uint32 m_num_write_columns;                     // Count of write columns
  Uint8 *m_bitmap_write_columns;                  // Bitmap for columns to write
  NdbBlob **m_write_blob_handles;                 // Blob handles for write columns
  bool m_has_write_blobs;                         // True if any write column is BLOB/TEXT
  Uint32 m_write_op_type;                         // RDRS_WRITE_OP_WRITE/_UPDATE/_INSERT
  ```

## BLOB Handling in Delete Operations

**Delete operations must handle BLOB part deletion.** When deleting rows from tables with BLOB/TEXT columns, the blob parts stored in separate tables must also be deleted. NDB handles this automatically through NdbBlob, but it must be activated by calling `getBlobHandle()`.

### How It Works

1. **Detection**: `setup_table_blob_handles()` scans ALL columns in the table (not just readColumns) for BLOB/TEXT types

2. **Activation**: In `setup_delete_operations()`, call `getBlobHandle()` for each BLOB column:
   ```cpp
   for (Uint32 blobIdx = 0; blobIdx < key_op->m_num_blob_columns; blobIdx++) {
     const NdbDictionary::Column *col = key_op->m_blobColumns[blobIdx];
     key_op->m_blob_handles[blobIdx] = operation->getBlobHandle(col->getName());
   }
   ```

3. **Execution**: NdbBlob handles everything internally during `execute(Commit)`:
   - **preExecute**: Adds read operation to get blob head (contains blob length)
   - **postExecute**: Processes blob head, queues blob part deletions
   - **handleBlobTask**: Deletes all blob parts from the blob parts table
   - **Commit**: Finalizes the transaction

### Key Points

- **Table-level vs readColumn-level**: Read operations check readColumns for blobs. Delete operations check ALL table columns.
- **No blob data in response**: `supports_blobs()` returns false - BLOB columns cannot be returned in delete response (only non-blob readColumns)
- **Automatic cleanup**: Once `getBlobHandle()` is called, NDB handles all blob part deletion automatically
- **Use Commit**: Unlike reads (which use NoCommit for blob data fetching), deletes use Commit directly since NdbBlob handles everything internally

### DeleteKeyOperation Extra Fields

```cpp
struct DeleteKeyOperation : public BaseKeyOperation {
  const NdbDictionary::Column **m_blobColumns;  // Table's blob columns
  Uint32 m_num_blob_columns;                     // Count of blob columns
};
```

## Early Validation

Validate constraints during request parsing, not during response creation:

```cpp
// In init_batch_operations, when validating readColumns:
if (!supports_blobs() &&
    (read_col->getType() == NdbDictionary::Column::Blob ||
     read_col->getType() == NdbDictionary::Column::Text)) {
  // Return error immediately
}
```

## Required Includes for Base Class

When creating shared base classes, these includes are commonly needed:

```cpp
#include "src/config_structs.hpp"  // globalConfigs
#include "src/encoding.hpp"        // getNextRespRS_Buffer
#include "src/rdrs_dal.h"          // RS_Status, RS_Buffer, etc.
#include "src/error_strings.h"     // rdrsErrorMessage
#include "src/rdrs_const.h"        // WAITFOR_RESPONSE_TIMEOUT, etc.
```

## File Locations

- Controllers: `server/src/` (e.g., `batch_pk_delete_ctrl.cpp`)
- DB operations: `server/src/db_operations/pk/`
- Base classes: `server/src/db_operations/pk/pk_base_operation.*`, `pk_batch_base_operation.*`

## Go Integration Tests

Integration tests for the REST API are located in `server/test_go/internal/integrationtests/`. Each endpoint has its own test package.

### Running Tests

Tests can be run via MTR (MySQL Test Runner):
```bash
cd debug_build/mysql-test
./mtr rdrs2-golang_batchwrite    # Run batchwrite tests
./mtr rdrs2-golang_batchdelete   # Run batchdelete tests
./mtr rdrs2-golang_batchpkread   # Run batch read tests
```

Or directly with Go (requires running RonDB cluster):
```bash
cd server/test_go
go test -v ./internal/integrationtests/batchwrite/
go test -v ./internal/integrationtests/batchdelete/
```

### Test Package Structure

Each test package follows this structure:
```
internal/integrationtests/<endpoint>/
├── wrapper_test.go    # TestMain - test initialization
├── utils.go           # Test utility functions
├── utils_rest.go      # REST-specific test helpers
└── handler_test.go    # Main test cases
```

### Batchwrite Tests (`batchwrite/`)

Tests for the `/0.1.0/batchwrite` endpoint supporting pk-write, pk-update, pk-insert operations.

| Test | Description |
|------|-------------|
| `TestBatchWriteSimple` | Basic pk-write and pk-update operations |
| `TestBatchWriteMultiple` | Multiple operations in a single batch |
| `TestBatchWriteUpdateNonExistent` | pk-update on non-existent row (expects 404) |
| `TestBatchWriteTextColumns` | VARCHAR column writes |
| `TestBatchWriteBlobColumns` | BLOB/TEXT column writes via NdbBlob |
| `TestBatchWriteMultipleBlobOps` | Multiple BLOB operations in one batch |
| `TestBatchWriteMissingReqField` | Validation of required request fields |
| `TestBatchWriteInvalidURLSuffix` | Invalid URL suffix error handling |
| `TestBatchWriteNonExistentTable` | Non-existent table error (404) |
| `TestBatchWriteNonExistentColumn` | Non-existent column error (400) |

### Batchdelete Tests (`batchdelete/`)

Tests for the `/0.1.0/batchdelete` endpoint. These tests follow a write-verify-delete-verify pattern to ensure proper deletion.

| Test | Description |
|------|-------------|
| `TestBatchDeleteIntTable` | Delete integer rows with full verification cycle |
| `TestBatchDeleteBigintTable` | Delete bigint rows |
| `TestBatchDeleteVarcharTable` | Delete varchar rows |
| `TestBatchDeleteVarbinaryTable` | Delete varbinary rows |
| `TestBatchDeleteTextTable` | Delete rows with TEXT columns (BLOB part cleanup) |
| `TestBatchDeleteBlobTable` | Delete rows with BLOB columns |
| `TestBatchDeleteNonExistent` | Delete non-existent row (expects 404) |
| `TestBatchDeleteMultiple` | Multiple deletes in single batch |
| `TestBatchDeleteAcrossTables` | Delete from different tables in one batch |
| `TestBatchDeleteMissingReqField` | Validation of required request fields |
| `TestBatchDeleteNonExistentTable` | Non-existent table error |
| `TestBatchDeleteInvalidURLSuffix` | Invalid URL suffix error |
| `TestBatchDeleteAndReinsert` | Delete and re-insert idempotency test |

### Helper Functions in Batchdelete Tests

The batchdelete tests use helper functions that combine multiple endpoints:

```go
// Write data using batchwrite endpoint
writeTestData(t, writeOps)

// Read data using batch endpoint to verify
readTestData(t, readOps)

// Verify row exists/deleted
verifyRowExists(t, db, table, filters)
verifyRowDeleted(t, db, table, filters)
```

### JSON Errors Tests (`jsonerrors/`)

Tests for the JSON error response format in API version 0.2.0:

| Test | Description |
|------|-------------|
| `TestJSONErrorResponseFormat` | Validates JSON error structure for various error conditions |
| `TestJSONErrorVsPlainTextError` | Compares 0.1.0 (plain text) vs 0.2.0 (JSON) error responses |
| `TestJSONErrorNonExistentTable` | JSON error for non-existent table operations |
| `TestJSONErrorMissingRequiredFields` | JSON errors for missing method, URL, body fields |

The tests verify the error response structure:
```json
{
  "error": {
    "code": 400,
    "message": "error message",
    "status": -1,
    "classification": -1,
    "ndbCode": -1,
    "mysqlCode": -1
  }
}
```

### Batchcomprehensive Tests (`batchcomprehensive/`)

Comprehensive integration tests (~1700 lines) covering all batch operations with extensive scenarios:

| Section | Tests |
|---------|-------|
| CRUD Lifecycle | `TestCRUDLifecycleInt`, `TestCRUDLifecycleBigint`, `TestCRUDLifecycleVarchar` |
| Batch Operations | `TestBatchMultipleWrites`, `TestBatchMultipleReads`, `TestBatchMultipleDeletes` |
| Cross-Table | `TestCrossTableBatchOperations` - operations on DB004, DB005, DB006, DB007 in one batch |
| Mixed Operations | `TestMixedWriteAndUpdate`, `TestPkInsertOperation` |
| Edge Cases | `TestNullValues`, `TestBoundaryValues`, `TestSpecialCharacters` (unicode, emoji, quotes) |
| Error Handling | `TestUpdateNonExistent`, `TestReadNonExistent`, `TestDeleteNonExistent`, `TestPartialBatchFailure` |
| Large Objects | `TestTextColumnOperations`, `TestBlobColumnOperations`, `TestVarbinaryOperations` |
| Large Batches | `TestLargeBatch` - 100 operations in single batch |
| Idempotency | `TestPkWriteIdempotency`, `TestDeleteAndReinsert` |

### Adding New Test Packages

1. Create test directory: `internal/integrationtests/<newpackage>/`
2. Add the four standard files (wrapper_test.go, utils.go, utils_rest.go, handler_test.go)
3. Add data structures to `pkg/api/` if needed
4. Add URL generators to `internal/testutils/url_generator.go`
5. Add constants to `internal/config/constants.go`
6. Add MTR mapping in `mysql-test/suite/rdrs2-golang/include/run_gotest.inc`
7. Create MTR test file: `mysql-test/suite/rdrs2-golang/t/rdrs2-golang_<newpackage>.test`
8. Create MTR result file: `mysql-test/suite/rdrs2-golang/r/rdrs2-golang_<newpackage>.result`

### Test Data Isolation

Tests run twice per MTR execution (once without TLS, once with TLS) on the same database. To avoid conflicts:
- Use unique primary keys per test (e.g., id values 8000+)
- Use pk-write instead of pk-insert for setup (handles existing rows)
- Clean up test data at end of test if needed

### API Data Structures (pkg/api/)

| File | Structures |
|------|------------|
| `pk-data-structs.go` | `Filter`, `ReadColumn`, `WriteColumn`, `PKReadBody`, `PKWriteBody` |
| `batch-data-structs.go` | `BatchOpRequest`, `BatchWriteOpRequest`, `BatchDeleteOpRequest`, `PKDeleteBody` |

## rondb-cli Integration

The `rondb-cli` tool (`tools/rondb-cli/`) provides a unified CLI for RonDB, supporting Rondis commands, SQL queries, and REST API BATCH operations.

### Building rondb-cli

rondb-cli is built automatically as part of the CMake build when Go is installed:

```bash
cd debug_build
make rondb-cli          # Build just rondb-cli
make                    # Or build everything
```

The binary is placed in `bin/rondb` under the build directory.

### Command-Line Flags for Scripted Usage

| Flag | Description |
|------|-------------|
| `-e, --execute` | Execute command and exit (non-interactive mode) |
| `-q, --quiet` | Suppress timing and info messages (deterministic output) |
| `--host` | RonDB host (sets both MySQL and RDRS hosts) |
| `--mysql-port` | MySQL port (default: 3306) |
| `--rdrs-port` | RDRS/REST API port (default: 4406) |
| `--rdrs-api-key` | API key for RDRS authentication |
| `--no-rdrs` | Disable RDRS/REST API connection |
| `--no-rondis` | Disable Rondis connection |
| `--no-mysql` | Disable MySQL connection |

### Examples

```bash
# SQL query (non-interactive)
rondb -e "SELECT * FROM db.table;" --quiet

# BATCH operation (requires API key for Hopsworks authentication)
rondb -e "BATCH WRITE db.table col=value FILTER id=1;" --rdrs-api-key=$API_KEY --quiet

# SQL-only mode (disable RDRS and Rondis)
rondb --no-rdrs --no-rondis -e "INSERT INTO db.table VALUES (1, 'test');"
```

### MTR Integration

rondb-cli is integrated with MTR (MySQL Test Runner) for testing:

1. **Environment variable**: MTR sets `RONDB_CLI` pointing to the rondb binary
2. **Connection parameters**: `RDRS_HOST`, `RDRS_PORT`, `MASTER_MYPORT` available in tests
3. **Test location**: `mysql-test/suite/rondb-cli/t/rondb_cli_batch.test` (dedicated suite)

### MTR Test Example

```sql
# Check rondb-cli is available
if (!$RONDB_CLI) {
  --skip rondb-cli binary not found
}

# SQL-only mode (no API key needed)
--let $RONDB_CMD=$RONDB_CLI --host=127.0.0.1 --mysql-port=$MASTER_MYPORT --no-rdrs --no-rondis --quiet

# Execute SQL via rondb-cli
--exec $RONDB_CMD -e "INSERT INTO test.t1 VALUES (1, 'Alice');"
--exec $RONDB_CMD -e "SELECT * FROM test.t1;"
```

### API Key Authentication

BATCH operations (via RDRS) require API key authentication when `UseHopsworksAPIKeys: true` is configured:

- Test API key: `bkYjEz6OTZyevbqt.ocHajJhnE0ytBh8zbYj3IXupyMqeMZp8PW464eTxzxqP5afBjodEQUgY0lmL33ub`
- The API key must be authorized for the target database in the hopsworks schema
- Go tests set up the hopsworks database with test API keys during `testdbs.CreateDataBases()`
- SQL-only tests can bypass this by using `--no-rdrs` flag

### Files Modified for rondb-cli Integration

| File | Change |
|------|--------|
| `tools/CMakeLists.txt` | Top-level tools directory |
| `tools/rondb-cli/CMakeLists.txt` | Go build integration with CMake |
| `CMakeLists.txt` | Added `ADD_SUBDIRECTORY(tools)` |
| `mysql-test/mysql-test-run.pl` | Added `RONDB_CLI` environment variable |
| `mysql-test/suite/rondb-cli/my.cnf` | Dedicated MTR suite for rondb-cli tests |
| `mysql-test/suite/rdrs2-golang/my.cnf` | Added `RDRS_HOST`, `RDRS_PORT` variables |

**Note**: rondb-cli tests are in a separate `rondb-cli` MTR suite (not `rdrs2-golang`) because they test the CLI binary directly rather than Go test packages.

## Hopsworks Schema Requirements

The hopsworks database tables used for API key authentication **must use `latin1` charset**. The RDRS code validates column sizes which depend on the charset.

### Required Charset

```sql
CREATE TABLE `api_key` (
  `prefix` varchar(45) NOT NULL,  -- Must be latin1 (45 bytes)
  ...
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;

CREATE TABLE `project_team` (
  `team_member` varchar(150) NOT NULL,  -- Must be latin1 (150 bytes)
  ...
) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
```

If tables use `utf8mb4` (default in MySQL 8), the column sizes are 4x larger and RDRS validation fails.

### Error Handling Pattern

**Prefer error returns over asserts** for schema validation. Asserts crash RDRS; error returns allow graceful failure with diagnostic messages.

```cpp
// Bad - crashes RDRS on schema mismatch
assert(col_size == API_KEY_PREFIX_SIZE);

// Good - returns error with helpful message
if (unlikely(col_size != API_KEY_PREFIX_SIZE)) {
  ndb_object->closeTransaction(tx);
  return RS_SERVER_ERROR(
    "hopsworks.api_key table has wrong schema: prefix column size mismatch "
    "(expected latin1 charset)");
}
```

Key validation points in `rdrs_hopsworks_dal.cpp`:
- Line ~89: `api_key.prefix` column size check
- Line ~386: `project_team.team_member` column size check

### MTR Test Setup

When creating hopsworks tables in MTR tests, always specify the charset:

```sql
CREATE TABLE `users` (...) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
CREATE TABLE `project` (...) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
CREATE TABLE `project_team` (...) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
CREATE TABLE `api_key` (...) ENGINE=ndbcluster DEFAULT CHARSET=latin1;
```

## Rondis Configuration

Rondis (Redis-compatible interface) can be configured to handle table initialization in different ways.

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `CreateTables` | `true` | Create Rondis databases and tables on startup if they don't exist |
| `RequireTablesOnStartup` | `true` | Initialize NDB records at startup; if `false`, records are lazily initialized on first command |

### Use Cases

**Production (default):**
```json
{
  "Rondis": {
    "CreateTables": true,
    "RequireTablesOnStartup": true
  }
}
```
Tables are created and records initialized at startup. Server fails to start if initialization fails.

**MTR Tests:**
```json
{
  "Rondis": {
    "CreateTables": false,
    "RequireTablesOnStartup": false
  }
}
```
Tables are created by the test via SQL. Records are lazily initialized on first Rondis command. This allows tests to control table creation timing.

### Lazy Initialization

When `RequireTablesOnStartup: false`, records are initialized lazily via `ensure_records_initialized()`:

```cpp
// In rondb.cc
int ensure_records_initialized(Ndb *ndb, Uint32 database_id) {
  // Fast path: already initialized
  if (g_records_initialized[database_id]) {
    return 0;
  }

  // Slow path: initialize with mutex protection
  std::lock_guard<std::mutex> lock(g_records_init_mutex);
  if (g_records_initialized[database_id]) {
    return 0;  // Double-check after lock
  }

  NdbDictionary::Dictionary *dict = ndb->getDictionary();
  if (init_string_records(dict, database_id) != 0) {
    return -1;
  }

  g_records_initialized[database_id] = true;
  return 0;
}
```

This is called in `rondb_redis_handler()` before processing any NDB command.

### Key Files

- `server/src/config_structs_def.hpp` - `RondisConfig` class with config options
- `storage/ndb/src/rondis/src/rondb.cc` - `setup_ndb_connection_for_rondis()`, `ensure_records_initialized()`
- `storage/ndb/src/rondis/include/rondb.h` - Function declarations

## Rondis MTR Tests

Rondis MTR tests are in `mysql-test/suite/rondis/`.

### Test Structure

```
mysql-test/suite/rondis/
├── my.cnf                              # Suite configuration
├── rondis_config_template.json         # RDRS config (CreateTables=false)
├── include/
│   └── create_rondis_tables.inc        # SQL to create redis_0 tables
├── t/
│   ├── rondis_basic.test               # Basic SET/GET/DEL/HSET tests
│   └── rondis_advanced.test            # Advanced operations
└── r/
    ├── rondis_basic.result             # Expected output
    └── rondis_advanced.result
```

### Table Creation Include File

Tests source `create_rondis_tables.inc` to create tables via SQL instead of relying on RDRS:

```sql
--source suite/rondis/include/create_rondis_tables.inc
```

The include file creates:
- `redis_0` database
- `redis_0.string_keys` - String key metadata
- `redis_0.hset_keys` - Hash key metadata
- `redis_0.string_values` - Large string values

### Test Cleanup

Tests must drop the `redis_0` database at the end to pass MTR's check-testcase:

```sql
# At end of test
--disable_query_log
DROP DATABASE IF EXISTS redis_0;
--enable_query_log
```

### Running Rondis Tests

```bash
cd debug_build/mysql-test
./mtr --suite=rondis              # Run all Rondis tests
./mtr rondis.rondis_basic         # Run specific test
./mtr --record rondis.rondis_basic  # Re-record baseline
```

### Known Issues

- **GETRANGE with negative indices**: May crash the server (needs investigation)
- **Empty string quoting**: `SET key ''` fails due to shell quoting in MTR exec
