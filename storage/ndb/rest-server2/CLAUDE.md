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

**Batch Read** (endpoint: `/0.1.0/batch`):
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

**Batch Delete** (endpoint: `/0.1.0/batchdelete`):
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

**Batch Write** (endpoint: `/0.1.0/batchwrite`):
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
| `pk-update` | `updateTuple()` | Update only, fails if row doesn't exist |
| `pk-insert` | `insertTuple()` | Insert only, fails if row already exists |

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
