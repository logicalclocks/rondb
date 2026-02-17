# Memory Booking System Design

## Overview

The ndbd_malloc library is extended with a **booking mechanism** that allows
callers to reserve memory of a certain type in advance. A booking guarantees
that the booked memory will be available when allocation is later performed.

Bookings are tracked in **words** (Uint32, 4 bytes) for fine-grained accounting.
The underlying page allocator continues to work in 32KiB pages (8192 words).

## Motivation

Some subsystems need guaranteed memory availability for future operations:

- **DataMemory**: During certain operations one needs to be certain that a
  specific amount of DataMemory will be available for subsequent allocations.
  Since DataMemory is a dedicated pool (m_min == m_max), the booking reserves
  within that dedicated pool.

- **QUERY_MEMORY**: SPJ query memory is shared with other resource groups.
  A booking must never exceed the resource's limits (m_max, priority-based
  shared access). The booking ensures that the memory will be there when
  needed, while respecting the library's limits.

## Key Properties

1. **Booking returns a key**: `book_memory()` returns a `Uint32` key (>0 on
   success, 0 on failure). This key must be presented when allocating.

2. **Booked memory is invisible without the key**: Non-keyed allocations
   cannot see or use booked memory. The existing getter functions
   (`get_resource_free_reserved`, `get_resource_free_shared`,
   `get_resource_free`) are modified to subtract booked amounts.

3. **Keyed allocations consume bookings**: When `alloc_page()` or
   `alloc_pages()` is called with a booking key, the allocation draws from
   the booked reservation.

4. **Remove booking releases only the remaining portion**: When a booking is
   removed via `remove_booking()`, any memory already allocated using that key
   stays as normal allocated memory. Only the unallocated (remaining) portion
   of the booking is released back to the available pool.

5. **Separate overlay counters**: Booking counters are separate from the
   existing accounting counters (`m_curr`, `m_in_use`, `m_free_reserved`,
   `m_shared_in_use`). This preserves all existing `check()` invariants.

## Data Structures

### Per-Resource Booking Fields (in Resource_limit)

```
m_booked_words           : Uint64 - Total words booked for this resource
m_booked_pages           : Uint32 - Page equivalent: ceil(m_booked_words / 8192)
m_booked_pages_reserved  : Uint32 - Booked pages from the reserved area
m_booked_pages_shared    : Uint32 - Booked pages from the shared area
```

### Booking Record

```
struct Booking {
  m_key              : Uint32 - Unique key (non-zero when active)
  m_resource_id      : Uint32 - Resource group ID
  m_total_words      : Uint64 - Originally booked word count
  m_remaining_words  : Uint64 - Words not yet allocated from this booking
  m_pages_reserved   : Uint32 - Remaining booked pages from reserved area
  m_pages_shared     : Uint32 - Remaining booked pages from shared area
};
```

### Global Booking State (in Resource_limits)

```
m_bookings[MAX_BOOKINGS] : Array of Booking records (MAX_BOOKINGS = 64)
m_next_booking_key       : Uint32 - Monotonically increasing key generator
m_shared_booked          : Uint32 - Total booked pages from shared pool
```

## API

### Low-Level (Ndbd_mem_manager)

```
Uint32 book_memory(Uint32 type, Uint64 words)
```
Book `words` of memory for resource group `type`. Returns a booking key
(>0) on success. Returns 0 if the booking cannot be satisfied within
the resource's limits.

```
void remove_booking(Uint32 booking_key)
```
Remove a booking. The remaining (unallocated) portion is released back
to the available pool. Already-allocated memory remains as normal
allocations.

The existing `alloc_page` and `alloc_pages` gain an optional
`booking_key` parameter (default 0). When non-zero, the allocation
draws from the booking's reservation.

### High-Level (Pool Allocator)

```
Uint32 lc_ndbd_pool_book(Uint64 words, Uint32 pool_id)
```
Book memory at the pool level. Delegates to `Ndbd_mem_manager::book_memory`.

```
void lc_ndbd_pool_remove_booking(Uint32 booking_key)
```
Remove a pool-level booking.

```
void *lc_ndbd_pool_malloc(size_t size, Uint32 pool_id, Uint32 thread_id,
                          bool clear_flag, Uint32 booking_key = 0)
```
The existing malloc gains an optional booking key. When the pool needs
a new 2MiB segment from the global memory manager, the booking key is
passed through to `alloc_pages`.

## Accounting Model

### Booking: Separate Overlay Counters

The existing counters track actual physical allocations:
- `m_curr` per resource: pages physically allocated
- `m_in_use` global: total pages physically allocated
- `m_free_reserved` global: reserved pages not yet physically allocated
- `m_shared_in_use` global: shared pages physically allocated

Booking adds overlay counters that are subtracted in the getter functions:
- `m_booked_pages` per resource: reduces `get_resource_free()`
- `m_booked_pages_reserved` per resource: reduces `get_resource_free_reserved()`
- `m_shared_booked` global: reduces `get_resource_free_shared()`

This means:
- `check()` invariants are preserved (they only look at existing counters)
- Non-keyed allocations see reduced availability (bookings are invisible)
- The booking doesn't touch physical memory or the buddy allocator

### Book Memory Flow

```
book_memory(resource_id, words):
  pages = ceil(words / 8192)

  Check: get_resource_free(id) >= pages
  Check: get_resource_free_reserved(id) + get_resource_free_shared(id) >= pages

  Split between reserved and shared:
    from_reserved = min(pages, get_resource_free_reserved(id))
    from_shared = pages - from_reserved

  Update per-resource:
    m_booked_words += words
    m_booked_pages += pages
    m_booked_pages_reserved += from_reserved
    m_booked_pages_shared += from_shared

  Update global:
    m_shared_booked += from_shared

  Return unique key
```

### Allocation with Booking Key Flow

```
alloc_pages(type, ..., booking_key):
  if booking_key != 0:
    Validate key matches resource group
    consume_booking(key, cnt * 8192)   // Reverses booking counters

  Normal allocation proceeds:
    Resource limit checks (now see restored availability for keyed portion)
    Buddy system allocation (physical pages)
    post_alloc_resource_pages (updates actual counters)

  Net effect: booking count decreases, actual count increases by same amount
```

### Consume Booking Logic

```
consume_booking(key, words):
  use_words = min(words, booking.remaining_words)
  booking.remaining_words -= use_words

  Compute page delta:
    old_pages = ceil((remaining + use_words) / 8192)
    new_pages = ceil(remaining / 8192)
    delta_pages = old_pages - new_pages

  Reverse delta from counters (reserved first):
    res_reverse = min(delta_pages, booking.pages_reserved)
    shr_reverse = delta_pages - res_reverse
    Update per-resource and global counters

  If remaining_words == 0: deactivate booking
```

### Remove Booking Flow

```
remove_booking(key):
  Reverse remaining booking counters:
    pages_remaining = booking.pages_reserved + booking.pages_shared
    m_booked_pages -= pages_remaining
    m_booked_pages_reserved -= booking.pages_reserved
    m_booked_pages_shared -= booking.pages_shared
    m_shared_booked -= booking.pages_shared
    m_booked_words -= booking.remaining_words

  Deactivate booking record

  Already-allocated memory (in m_curr) is unaffected.
```

## Examples

### DataMemory (reserved, m_min=50, m_max=50, m_curr=30)

```
1) book_memory(RG_DM, 81920)  // 10 pages
   booked_pages=10, booked_pages_reserved=10
   Visible free reserved: 50-30-10 = 10 (was 20)
   Returns key K

2) alloc_page(RG_DM, ..., K)
   consume_booking: booked reduced by 1 page
   post_alloc: m_curr=31
   Visible free reserved: 50-31-9 = 10

3) remove_booking(K) with 9 pages remaining
   booked_pages -= 9, booked_pages_reserved -= 9
   m_curr stays at 31 (the 1 allocated page is normal)
   Visible free reserved: 50-31-0 = 19
```

### QUERY_MEMORY (shared, m_min=0, m_max=HIGHEST, shared_free=1000)

```
1) book_memory(RG_QM, 819200)  // 100 pages
   booked_pages=100, booked_pages_shared=100, m_shared_booked=100
   Shared free visible to ALL resources: 1000-0-100 = 900
   Returns key K

2) alloc_pages(RG_QM, ..., 64 pages, K)
   consume_booking: reverses 64 pages from booking
   Normal alloc: m_curr += 64, m_shared_in_use += 64
   Booking remaining: 36 pages

3) remove_booking(K) with 36 pages remaining
   m_shared_booked -= 36
   Shared free restored by 36 for all resources
   The 64 allocated pages stay as normal allocations
```

## Invariants

The following invariants are maintained at all times:

Existing (unchanged):
- sum(m_curr) == m_in_use
- sum(m_min + m_spare) == m_reserved
- m_reserved == sum(reserved_alloc) + m_free_reserved
- m_shared_in_use == sum(shared_alloc)

New:
- For each resource: m_booked_pages == m_booked_pages_reserved + m_booked_pages_shared
- For each resource: m_booked_pages == ceil(m_booked_words / 8192)
- sum(m_booked_pages_shared) == m_shared_booked
- For each active booking: remaining_words <= total_words
- For each active booking: pages_reserved + pages_shared == ceil(remaining_words / 8192)

## Thread Safety

All booking operations (book_memory, remove_booking, consume_booking) are
protected by the existing `mt_mem_manager_lock()` mutex, same as all other
memory management operations. No additional synchronization is needed.
