# SUMA Block

SUMA (SUbscription MAnager) handles event subscriptions and replication in NDB Cluster. It buffers data changes and sends them to API subscribers, with support for node failure takeover.

## Key Files

- `Suma.hpp` - Class definitions, data structures, constants
- `Suma.cpp` - Implementation

## Buffer Page Structure

`Buffer_page` (Suma.hpp:721) stores buffered event data:

```
DATA_WORDS = 8181 words per page
GCI_SZ32 = 2 (words for GCI)
SAME_GCI_FLAG = 0x80000000
SIZE_MASK = 0x0000FFFF
PART_NUM_SHIFT = 28
PART_NUM_MASK = 7
```

### Entry Header Word Format

```
Bits 0-15:  Size (total words including header)
Bits 28-30: Part number (0-5)
Bit 31:     SAME_GCI_FLAG (if clear, 2 GCI words follow header)
```

## Event Record Parts

A single event record is split across multiple parts:

| Part | Content | Size |
|------|---------|------|
| 0 | GCI completion marker | sz=0 |
| 1 | Header (7 words) + primary key | Variable |
| 2 | After values - continuation | MAX_SUMA_BUFFER_SIZE (4000) |
| 3 | After values - final chunk | Variable |
| 4 | Before values - continuation | MAX_SUMA_BUFFER_SIZE (4000) |
| 5 | Before values - final chunk | Variable |

### Part 1 Header Layout (buffer_header_sz = 7)

```c
[0] subPtrI           // Subscription pool index
[1] schemaVersion     // Table schema version
[2] (event << 16) | key_sz  // Event type + key size
[3] any_value         // Trigger any_value
[4] transId1          // Transaction ID part 1
[5] transId2          // Transaction ID part 2
[6] subAutoIncrement  // Subscription validation token
// Followed by: primary key data (key_sz words)
```

## Key Functions

### `resend_bucket` (line ~7981)

Resends buffered data after node takeover. Called via CONTINUEB for incremental processing.

**Parameters:**
- `buck` - Bucket number (0 to NO_OF_BUCKETS-1)
- `min_gci` - Minimum GCI to resend (skip older)
- `pos` - Current position in tail page
- `last_gci` - Last seen GCI (for SAME_GCI_FLAG)

**Flow:**
1. Validate resendability (check `normal_resendable()`)
2. Scan page entries, skip entries with `last_gci < min_gci`
3. For GCI markers (part=0, sz=0): send `SubGcpCompleteRep`
4. For data records (part=1): reassemble parts 2-5, validate subscription, send `SubTableData`
5. Free completed pages, schedule next iteration

### `get_buffer_ptr` (line ~6977)

Allocates space in buffer pages for writing event data.

**Parameters:**
- `buck` - Bucket number
- `gci` - Global checkpoint ID
- `sz` - Size needed (words)
- `part` - Part number (0-5)

**Returns:** Pointer to write data (after header word and optional GCI)

### `doFIRE_TRIG_ORD` (line ~5124)

Handles trigger events. Either sends immediately (active bucket) or buffers for resend.

**Write sequence for buffering:**
1. Part 1: header + keys
2. Parts 2/3: after values (lsptr[2])
3. Parts 4/5: before values (lsptr[1])

## Bucket Management

- `NO_OF_BUCKETS = 24` - Total buckets distributed across nodes
- `m_active_buckets` - Buckets this node handles
- `m_switchover_buckets` - Buckets in takeover transition
- Each bucket has a `m_buffer_tail` (oldest) and `m_buffer_head` (newest)

## LinearSectionPtr Mapping

In trigger handling:
```c
lsptr[0] = primary key data
lsptr[1] = before values (b_buffer)
lsptr[2] = after values (f_buffer)
```
