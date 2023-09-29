/*
   Copyright (c) 2006, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef NDBD_MALLOC_IMPL_H
#define NDBD_MALLOC_IMPL_H

#include "my_config.h"
#include "util/require.h"
#include <algorithm>
#ifdef VM_TRACE
//#ifndef NDBD_RANDOM_START_PAGE
//#define NDBD_RANDOM_START_PAGE
//#endif
#endif

#include <cstdint>
#include <kernel_types.h>
#include <Bitmask.hpp>
#include <assert.h>
#include "NdbSeqLock.hpp"
#include "Pool.hpp"
#include <Vector.hpp>
#include <EventLogger.hpp>

#define JAM_FILE_ID 291

#ifdef NDBD_RANDOM_START_PAGE
extern Uint32 g_random_start_page_id;
#endif

/**
 * Ndbd_mem_manager handles memory in pages of size 32KiB.
 *
 * Pages are arranged in 8GiB regions, there first page is a bitmap
 * indicating what pages in region have free page data, the last page is
 * not used.
 *
 * There is one base address and pages are numbered with an 32 bit index
 * from that address.  Index should be less than RNIL (0xFFFFFF00).  RNIL
 * is not a valid page number.
 *
 * Regions are numbered with a 14 bit number, there 0x3FFF may not be
 * used.  This limit possible page numbers to 0xFFFC0000.
 *
 * Furthermore there are zones defined that contains pages which have a
 * page number representable with some specific number of bits.
 *
 * There are currently four zones:
 *
 * ZONE_19: regions      0 - 1       , pages      0 - (2^19-1)
 * ZONE_27: regions      2 - (2^9-1) , pages (2^19) - (2^27-1)
 * ZONE_30: regions  (2^9) - (2^12-1), pages (2^27) - (2^30-1)
 * ZONE_32: regions (2^12) - 0x3FFE  , pages (2^30) - 0xFFFBFFFF
 */

/**
 * 13 -> 8192 words -> 32768 bytes
 * 18 -> 262144 words -> 1M
 */
#define BMW_2LOG 13
#define BITMAP_WORDS (1 << BMW_2LOG)

#define BPP_2LOG (BMW_2LOG + 5)
#define PAGE_REGION_MASK ((1 << BPP_2LOG) - 1)
#define SPACE_PER_BMP_2LOG ((2 + BMW_2LOG) + BPP_2LOG)

struct Alloc_page 
{
  Uint32 m_data[BITMAP_WORDS];
};

struct InitChunk
{
  Uint32 m_cnt;
  Uint32 m_start;
  Alloc_page* m_ptr;
};

struct Free_page_data 
{
  Uint32 m_list;
  Uint32 m_next;
  Uint32 m_prev;
  Uint32 m_size;
};

#define FPD_2LOG 2

#define MM_RG_COUNT 12

/**
  Information of restriction and current usage of shared global page memory.
*/
class Resource_limits
{
  /**
   * Number of pages reserved for specific resource groups but currently
   * not in use.
   */
  Uint32 m_free_reserved;

  /**
   * Total number of pages reserved for all resource groups.
   */
  Uint32 m_reserved;

  /**
   * Total number of pages in shared global memory.
   */
  Uint32 m_shared;

  /**
   * Number of pages in shared global memory currently in use.
   */
  Uint32 m_shared_in_use;

  /**
   * Number of pages currently in use.
   */
  Uint32 m_in_use;

  /**
   * Total number of pages allocated.
   */
  Uint32 m_allocated;

  /**
    One more than highest page number allocated.

    Used internally by Ndbd_mem_manager for consistency checks.
  */
  Uint32 m_max_page;

  /**
    Number of pages reserved for high priority resource groups.

    Ultra prio free limit is memory reserved for things that are
    of survival necessity. This includes allocations for job buffer,
    send buffers, backup meta memory and some other minor things.

    Most other memory resources are in the category of prioritized, but
    can survive a memory request failure.

    Finally we have low priority memory resources, this is mainly intended
    for complex SQL queries handled by SPJ.

    Page allocations for low priority resource groups will be denied if number
    of free pages are less than this number.
  */
  Uint32 m_prio_free_limit;
  Uint32 m_ultra_prio_free_limit;

  /**
    Per resource group statistics.
    See documentation for Resource_limit.
  */
  Resource_limit m_limit[MM_RG_COUNT];

  /**
    Keep 10% of unreserved shared memory only for high priority resource
    groups.

    High priority of a resource group is indicated by setting the minimal
    reserved to zero (Resource_limit::m_min == 0).
  */
  static const Uint32 ULTRA_PRIO_FREE_PCT = 4;
  static const Uint32 HIGH_PRIO_FREE_PCT = 10;

  void dec_in_use(Uint32 cnt);
  void dec_resource_in_use(Uint32 id, Uint32 cnt);
  Uint32 get_resource_in_use(Uint32 resource) const;
  void inc_free_reserved(Uint32 cnt);
  void inc_in_use(Uint32 cnt);
  void inc_resource_in_use(Uint32 id, Uint32 cnt);

  /**
   * Increment number of pages this resource has stolen from the
   * reserved area.
   */
  void inc_stolen_reserved(Uint32 id, Uint32 cnt);

  /**
   * Decrement number of pages this resource has stolen from the
   * reserved area.
   */
  void dec_stolen_reserved(Uint32 id, Uint32 cnt);

  /**
   * Get number of pages this resource has stolen from the
   * reserved area.
   */
  Uint32 get_stolen_reserved(Uint32 id) const;

  /**
   * Increment number of pages this resource has extended the
   * reserved area by through calls to alloc_emergency_page.
   */
  void inc_overflow_reserved(Uint32 id, Uint32 cnt);

  /**
   * Decrement number of pages this resource has extended the
   * reserved area by through calls to alloc_emergency_page.
   */
  void dec_overflow_reserved(Uint32 id, Uint32 cnt);

  /**
   * Get number of pages this resource has extended the
   * reserved area by through calls to alloc_emergency_page.
   */
  Uint32 get_overflow_reserved(Uint32 id) const;

public:
  Resource_limits();

  void get_resource_limit(Uint32 id, Resource_limit& rl) const;
  void init_resource_limit(Uint32 id,
                           Uint32 min,
                           Uint32 max,
                           Uint32 max_high_prio,
                           Uint32 prio);
  void init_resource_spare(Uint32 id, Uint32 pct);

  /**
   * Get total number of allocated pages in global memory manager.
   */
  Uint32 get_allocated() const;

  /**
   * Get total number of reserved pages in global memory manager.
   */
  Uint32 get_reserved() const;

  /**
   * Get total number of pages in shared global memory.
   */
  Uint32 get_shared() const;

  /**
   * Get total number of pages used in shared global memory.
   */
  Uint32 get_shared_in_use() const;

  Uint32 get_free_reserved() const;
  Uint32 get_free_shared() const;
  
  /**
   * Get total number of pages in use in the global memory manager.
   */
  Uint32 get_in_use() const;
  Uint32 get_reserved_in_use() const;

  /**
   * The page id of the last page in the global memory manager.
   */
  Uint32 get_max_page() const;

  /**
   * Get number of pages that are free in the resource until we have
   * reached the maximum allowed usage.
   */
  Uint32 get_resource_free(Uint32 id, bool use_spare) const;

  /**
   * Get number of free pages in the reserved part of the resource.
   */
  Uint32 get_resource_free_reserved(Uint32 id, bool use_spare) const;

  /**
   * Get number of free pages in shared global memory as seen by this
   * resource group.
   */
  Uint32 get_resource_free_shared(Uint32 id) const;

  /**
   * Get number of reserved pages in resource group.
   * To get actual number of reserved pages one need to
   * add spare pages as well since they are also part of
   * reserved pages although not always accessible.
   */
  Uint32 get_resource_reserved(Uint32 id) const;

  /**
   * Get number of spare pages in resource group.
   */
  Uint32 get_resource_spare(Uint32 id) const;

  /**
   * Decrement number of pages in use in shared global memory.
   */
  void dec_shared_in_use(Uint32 cnt);

  /**
   * Increment number of pages in use in shared global memory.
   */
  void inc_shared_in_use(Uint32 cnt);

  /**
   * Decrease number of free reserved pages.
   */
  void dec_free_reserved(Uint32 cnt);

  /**
   * Set the page id of the last page in the global memory manager.
   */
  void set_max_page(Uint32 page);

  /**
   * Set the total number of pages allocated in the global memory
   * manager, set in init and map calls.
   */
  void set_allocated(Uint32 cnt);

  /**
   * Set high prio and ultra prio free limits.
   */
  void set_prio_free_limits(Uint32);

  /**
   * Set number of pages in shared global memory.
   */
  void set_shared();

  /**
   * This function is called after successfully allocating a set of
   * pages in alloc_page and alloc_pages. It updates the Global view
   * of memory usage as well as the view per Resource group.
   */
  void post_alloc_resource_pages(Uint32 id, Uint32 cnt);
  void post_release_resource_pages(Uint32 id, Uint32 cnt);
  void post_alloc_resource_emergency(Uint32 id, Uint32 cnt, bool);

  void check() const;
  void dump() const;
};

class Ndbd_mem_manager 
{
  friend class Test_mem_manager;
public:
  Ndbd_mem_manager();
  
  /**
   * This call is executed once for each memory region to initialize the
   * resource limits for the region.
   */
  void set_resource_limit(const Resource_limit& rl);

  /**
   * This call initialize the prio free limits, this is a global setting
   * that contains the number of pages that can only be allocated from regions
   * with ULTRA_PRIO_MEMORY set and also for calls to alloc_emergency_page.
   * It also sets the limit of the high priority memory region.
   */
  void set_prio_free_limits(Uint32);

  /**
   * get_resource_limit is used in a number of places for debugging, to print
   * information about state of global memory manager, to issue information
   * in ndbinfo calls and in various algorithmic parts where it is important
   * to know about certain sizes.
   *
   * Also used to discover send buffer level to see if we are in a critical
   * state where we are running out of send buffers. In this particular
   * case the memory is read without using the mutex.
   */
  bool get_resource_limit(Uint32 id, Resource_limit& rl) const;
  bool get_resource_limit_nolock(Uint32 id, Resource_limit& rl) const;

  /**
   * Get the number of reserved pages in the resource group
   */
  Uint32 get_reserved(Uint32 id);
  /**
   * get_allocated is used by one DUMP command and in the resources table in
   * ndbinfo.
   */
  Uint32 get_allocated() const;

  /**
   * get_reserved, get_reserved_in_use, get_shared, get_shared_in_use and
   * get_spare are only used by a DUMP command.
   * The command is DUMP 1000 0 0
   * This command prints the information about the total memory resources
   * and their state.
   */
  Uint32 get_reserved() const;
  Uint32 get_reserved_in_use() const;
  Uint32 get_shared() const;
  Uint32 get_shared_in_use() const;

  /**
   * get_free_shared is only used internally, get_free_shared_nolock is
   * used externally in mt.cpp to retrieve the send buffer level we are
   * currently at.
   */
  Uint32 get_free_shared() const;
  Uint32 get_free_shared_nolock() const;

  /**
   * get_in_use is used by the resources ndbinfo table and by the above
   * mentioned DUMP 1000 command.
   */
  Uint32 get_in_use() const;

  /**
   * init initialises the global memory manager, this includes allocating the
   * memory.
   * map touches the memory, sets up the internal data structures of the global
   * memory manager and locks the memory also if required.
   *
   * After initialising the global memory manager we initialize the memory pools
   * that can be used using a malloc-like interface.
   */
  bool init(Uint32 *watchCounter,
            Uint32 pages,
            bool allow_alloc_less_than_requested = true);
  void map(Uint32 * watchCounter,
           bool memlock = false,
           Uint32 resources[] = 0);
  void init_memory_pools();

  /**
   * init_resource_spare is called to ensure that Data memory is no longer
   * using the full data memory unless calling alloc_page with the spare
   * flag set. The spare flag is set when using alloc_page during restarts.
   * It is normal that the data memory is a bit bigger during restoring the
   * data. To handle this we save 5% by default of DataMemory that is only
   * available during restarts.
   */
  void init_resource_spare(Uint32 id, Uint32 pct);

  /**
   * get_memroot retrieves the memory address of the very first page in the
   * global memory manager. Thus every word in the global memory manager can
   * be accessed using (Uint32*(get_memroot()) + page_id * 8192 + page_index.
   */
  void* get_memroot() const;

  /**
   * Calls to control debugging of crashes involving the global memory
   * manager.
   */
  void dump(bool locked) const ;
  void dump_on_alloc_fail(bool on);

  /**
   * In some cases we have stored page id's with less than 32 bits of space.
   * These page id's cannot be used to access any memory, for example a
   * 19-bit page id can be used to allocate memory from the first 16 GByte
   * of memory, the 27-bit pointers are used to allocate from the first
   * 4 TByte of memory and the 30-bit pointers can be used to address the
   * first 32 TBytes of memory and finally 32-bit pointers can be used to
   * address 128 TByte of memory.
   *
   * Most of the DataMemory uses 30-bit pointers since we use 2 bits in
   * the page maps to contain state used by e.g. local checkpoints.
   * Thus going beyond 32 TBytes of space in a single node will require some
   * adaption of those page maps such that they contain at least 32 bits of
   * page id and thus extending memory space to 128 TByte. This should
   * suffice for 10 years or so. After that one will have to store pointers
   * that are larger than 32 bits in various places.
   * The references in the database (in hash index, ordered index, references
   * to other parts of the row (varsize and disk part of the row) are 8-bytes
   * long. So there is plenty of space to grow beyond 32 bits page ids when
   * the time for this arrives.
   *
   * Most data structures have been converted RWPool64 except for those data
   * structures that require a 32-bit page id and require substantial amounts
   * of memory. Since signal sending is still 32-bit arrays, there needs to
   * be some handling of 64-bit references in various protocols to grow those
   * spaces beyond 32 bit references. However we can thus have up to 4G
   * records in each block, thus most internal data structures can grow to
   * 100's of GBytes per block instance. Thus it is very unlikely that this
   * becomes a memory issue in the next 20 years at least.
   *
   * RWPool64 address records using the memroot and the 64 bit i-value
   * contains a page id and a page index. The page index is 13 bits and
   * the page id can be up to 51 bits, thus RWPool64 can address up to
   * 64 EB (Exabyte == 1.000.000 TB). This should be sufficient for a
   * very long time.
   */
  enum AllocZone
  {
    NDB_ZONE_LE_19 = 0, // Only allocate with page_id < (1 << 19)
    NDB_ZONE_LE_27 = 1,
    NDB_ZONE_LE_30 = 2,
    NDB_ZONE_LE_32 = 3,
  };

  /* get_page uses memroot and page id to get address to a page */
  void* get_page(Uint32 i) const; /* Note, no checks, i must be valid. */
  void* get_valid_page(Uint32 i) const; /* DO NOT USE see why at definition */

  /**
   * These are the main routines used by the global memory manager to manage
   * its memory.
   * alloc_page requests allocation of a single page.
   * alloc_pages requests allocation of multiple pages, with a desired count
   * and a minimum count.
   * release_page releases a single page
   * release_pages release a set of pages.
   *
   * All calls specify the type (the memory region) to use.
   * All calls have a flag indicating whether the memory manager is locked
   * already or not. Some variants require multiple calls while holding
   * the lock.
   *
   * All allocation calls specify the Zone to use for allocation. We always
   * attempt to allocate from the highest possible region first and then
   * proceed downwards until we reach the lowest region. Each of the regions
   * have their own set of free lists.
   *
   * All allocation calls will return pages in consecutive order, thus we
   * only return one page id in the i variable.
   *
   * alloc_page has a special flag called use_max_part. This is used
   * by mt.cpp for the send buffer. It says that if this is true we
   * should only allocate reserved pages. This makes it possible for
   * the send thread to see if it can steal pages from another send
   * thread to avoid allocating from the global memory manager. If
   * this fails it will call again to retrieve a page also from the
   * shared global memory.
   *
   * alloc_pages have count variable called cnt, this is input with
   * the number of desired pages to return, it is also an output
   * variable that indicates how many pages that was actually
   * allocated. Finally the min variable indicates the minimum
   * number of pages to allocate.
   *
   * All release calls have an i variable containing the page id of
   * the first released page.
   *
   * release_pages has a count variable called cnt, this indicates
   * the number of pages that are released.
   */
  void* alloc_page(Uint32 type,
                   Uint32* i,
                   enum AllocZone,
                   bool allow_use_spare_page = false,
                   bool locked = false,
                   bool use_max_part = true);
  void alloc_pages(Uint32 type,
                   Uint32* i,
                   Uint32 *cnt,
                   Uint32 min = 1,
                   AllocZone zone = NDB_ZONE_LE_32,
                   bool locked = false);
  void release_page(Uint32 type, Uint32 i, bool locked = false);
  void release_pages(Uint32 type, Uint32 i, Uint32 cnt, bool locked = false);

  /**
   * There are a number of cases where we might need to allocate pages
   * even beyond what the reserved and shared global memory allows for.
   *
   * In DBACC we need to be able to allocate pages from DataMemory during
   * expand and shrink calls, we cannot handle failures in this particular
   * call chain.
   *
   * In DBTUP and DBACC we want to enable tables that are undergoing
   * fragmentation changes to be able to access the extra 5% of space.
   * This ensures that those operations have a higher probaility of
   * success.
   *
   * Page maps in DBTUP use DataMemory as well, for simplicity these are
   * always ok with using the spare pages. Only the resource group for
   * DataMemory have spare pages, so for other resource groups using the
   * same underlying pools will not be affected.
   *
   * Similarly we want to have access to those 5% during restart.
   *
   * During a local checkpoint we might be in a situation that requires
   * allocating a page to store LCP related information of dropped
   * pages. This must not fail since that would fail the whole
   * LCP which isn't allowed. This call allocates a page in
   * TransactionMemory, but can in this rare case allocate from
   * also other areas to ensure that the allocation doesn't fail.
   *
   * The solution to this is to have a flag in alloc_page that indicates
   * if we are allowed to use spare pages in the resource. This is used
   * during restarts and during reorganization of the fragments.
   *
   * In critical situations we instead use alloc_emergency_page, this will
   * allocate a page also from shared global memory if required.
   *
   * The FORCE_RESERVED flag below can be used to test alloc_emergency_page
   * functionality where we steal pages from the reserved memory rather
   * than from the shared global memory.
   */
#define FORCE_RESERVED false
  void* alloc_emergency_page(Uint32 type,
                             Uint32* i,
                             enum AllocZone,
                             bool locked,
                             bool force_reserved);

  /**
   * Lock/Unlock the global memory manager for multi-call
   * scenarios.
   */
  void lock();
  void unlock();
  void check() const;

  /**
   * Compute 2log of size 
   * @note size = 0     -> 0
   * @note size > 65536 -> 16
   */
  static Uint32 ndb_log2(Uint32 size);

private:
  enum { ZONE_19 = 0, ZONE_27 = 1, ZONE_30 = 2, ZONE_32 = 3, ZONE_COUNT = 4 };
  enum : Uint32 {
    ZONE_19_BOUND = (1U << 19U),
    ZONE_27_BOUND = (1U << 27U),
    ZONE_30_BOUND = (1U << 30U),
    ZONE_32_BOUND = (RNIL)
  };

  struct PageInterval
  {
    PageInterval(Uint32 start = 0, Uint32 end = 0)
    : start(start), end(end) {}
    static int compare(const void* x, const void* y);

    Uint32 start; /* inclusive */
    Uint32 end; /* exclusive */
  };

  static const Uint32 zone_bound[ZONE_COUNT];
  void grow(Uint32 start, Uint32 cnt);
  bool do_virtual_alloc(Uint32 pages,
                        InitChunk chunks[ZONE_COUNT],
                        Uint32* watchCounter,
                        Alloc_page** base_address);

  /**
   * Return pointer to free page data on page
   */
  static Free_page_data* get_free_page_data(Alloc_page*, Uint32 idx);
  Vector<Uint32> m_used_bitmap_pages;
  
  Uint32 m_buddy_lists[ZONE_COUNT][16];
  Resource_limits m_resource_limits;
  Alloc_page * m_base_page;
#ifdef NDBD_RANDOM_START_PAGE
  Uint32 m_random_start_page_id;
#endif
  bool m_dump_on_alloc_fail;

  /**
   * m_mapped_page is used by get_valid_page() to determine what pages are
   * mapped into memory.
   *
   * This is normally not changed but still some thread safety is needed for
   * the rare cases when changes do happen whenever map() is called.
   *
   * A static array is used since pointers can not be protected by NdbSeqLock.
   *
   * Normally all page memory are allocated in one big chunk, but in debug mode
   * with do_virtual_alloc activated there will be at least one chunk per zone.
   * An arbitrary factor 3 is used to still handle with other unseen allocation
   * patterns.
   *
   * Intervals in m_mapped_pages consists of interval start (inclusive) and
   * end (exclusive).  No two intervals overlap.  Intervals are sorted on start
   * page number with lowest number first.
   */
  mutable NdbSeqLock m_mapped_pages_lock;
  Uint32 m_mapped_pages_count;
  /**
   * m_mapped_pages[0 to m_mapped_pages_count - 1] is protected by seqlock.
   * upper part is protected by same means as m_mapped_pages_new_count.
   */
  PageInterval m_mapped_pages[ZONE_COUNT * 3];
  /**
   * m_mapped_pages_new_count is not protected by seqlock but depends on calls
   * to grow() via map() is serialized by other means.
   */
  Uint32 m_mapped_pages_new_count;

  void release_impl(Uint32 zone, Uint32 start, Uint32 cnt);  
  void insert_free_list(Uint32 zone, Uint32 start, Uint32 cnt);
  Uint32 remove_free_list(Uint32 zone, Uint32 start, Uint32 list);

  void set(Uint32 first, Uint32 last);
  void clear(Uint32 first, Uint32 last);
  void clear_and_set(Uint32 first, Uint32 last);
  Uint32 check(Uint32 first, Uint32 last);

  static Uint32 get_page_zone(Uint32 page);
  void alloc(AllocZone, Uint32* ret, Uint32 *pages, Uint32 min_requested);
  void alloc_impl(Uint32 zone, Uint32* ret, Uint32 *pages, Uint32 min);
  void release(Uint32 start, Uint32 cnt);

  /**
   * This is memory that has been allocated
   *   but not yet mapped (i.e it is not possible to get it using alloc_page(s)
   */
  Vector<InitChunk> m_unmapped_chunks;
};

/**
 * Resource_limits
 */

inline
void
Resource_limits::post_alloc_resource_pages(Uint32 id, Uint32 cnt)
{
  const Uint32 inuse = get_resource_in_use(id);
  const Uint32 reserved = get_resource_reserved(id) +
                          get_resource_spare(id);
  Uint32 shared_cnt = 0;
  if (inuse < reserved)
  {
    Uint32 res_cnt = reserved - inuse;
    if (res_cnt >= cnt)
    {
      res_cnt = cnt;
    }
    else
    {
      /**
       * We will take parts from reserved memory and parts of it
       * from shared global memory.
       */
      shared_cnt = cnt - res_cnt;
      inc_shared_in_use(shared_cnt);
    }
    dec_free_reserved(res_cnt);
  }
  else
  {
    /**
     * All parts of the reserved space is already taken,
     * Increment shared in use.
     */
    inc_shared_in_use(cnt);
  }
  inc_resource_in_use(id, cnt);
  inc_in_use(cnt);
}

/**
 * Can only be called with cnt == 1, only called from
 * alloc_emergency_page
 */
inline
void
Resource_limits::post_alloc_resource_emergency(Uint32 id,
                                               Uint32 cnt,
                                               bool force_reserved)
{
  if (m_shared > m_shared_in_use && !force_reserved)
  {
    /**
     * A high priority resource was grabbed from the shared
     * global memory. Simply update the in use variables.
     */
    inc_shared_in_use(cnt);
  }
  else
  {
    /**
     * We have run out of shared global memory. We must steal
     * a page from the reserved area instead.
     * We need to remember that this page have been stolen
     * from reserved memory to handle the case when pages are
     * released from this resource group later on.
     */
    require(m_free_reserved > 0);
    inc_stolen_reserved(id, cnt);
    dec_free_reserved(cnt);
  }
  inc_overflow_reserved(id, cnt);
  inc_resource_in_use(id, cnt);
  inc_in_use(cnt);
}

inline
void Resource_limits::dec_shared_in_use(Uint32 cnt)
{
  assert(m_shared_in_use >= cnt);
  m_shared_in_use -= cnt;
}

inline
void Resource_limits::inc_shared_in_use(Uint32 cnt)
{
  assert((m_shared_in_use + cnt) <= m_shared);
  m_shared_in_use += cnt;
}

inline
void Resource_limits::dec_free_reserved(Uint32 cnt)
{
  assert(m_free_reserved >= cnt);
  m_free_reserved -= cnt;
}

inline
void Resource_limits::dec_in_use(Uint32 cnt)
{
  assert(m_in_use >= cnt);
  m_in_use -= cnt;
}

inline
void Resource_limits::dec_resource_in_use(Uint32 id, Uint32 cnt)
{
  assert(m_limit[id - 1].m_curr >= cnt);
  m_limit[id - 1].m_curr -= cnt;
}

inline
Uint32 Resource_limits::get_allocated() const
{
  return m_allocated;
}

inline
Uint32 Resource_limits::get_reserved() const
{
  return m_reserved;
}

inline
Uint32 Resource_limits::get_shared() const
{
  const Uint32 reserved = get_reserved();
  const Uint32 allocated = get_allocated();
  if (allocated < reserved)
    return 0;
  return allocated - reserved;
}

inline
Uint32 Resource_limits::get_free_reserved() const
{
  return m_free_reserved;
}

inline
Uint32 Resource_limits::get_free_shared() const
{
  Uint32 shared = m_shared;
  Uint32 shared_in_use = m_shared_in_use;
  /*
   * When called from get_free_shared_nolock ensure that total is not less
   * than used.
   */
  if (unlikely(shared < shared_in_use))
  {
    return 0;
  }
  return shared - shared_in_use;
}

inline
Uint32 Resource_limits::get_in_use() const
{
  return m_in_use;
}

inline
Uint32 Resource_limits::get_reserved_in_use() const
{
  return m_reserved - m_free_reserved;
}

inline
Uint32 Resource_limits::get_shared_in_use() const
{
  return m_shared_in_use;
}

inline
Uint32 Resource_limits::get_max_page() const
{
  return m_max_page;
}

inline
Uint32 Resource_limits::get_resource_free(Uint32 id, bool use_spare) const
{
  require(id <= MM_RG_COUNT);
  const Resource_limit& rl = m_limit[id - 1];
  Uint32 spare = use_spare ? rl.m_spare : 0;
  Uint32 used_reserve = rl.m_max + spare;
  if (used_reserve > rl.m_curr)
  {
     return (used_reserve - rl.m_curr);
  }
  return 0;
}

inline
Uint32
Resource_limits::get_resource_free_reserved(Uint32 id, bool use_spare) const
{
  require(id <= MM_RG_COUNT);
  const Resource_limit& rl = m_limit[id - 1];
  Uint32 spare = use_spare ? rl.m_spare : 0;
  Uint32 used_reserve = rl.m_min + spare;
  if (used_reserve > rl.m_curr)
  {
     return (used_reserve - rl.m_curr);
  }
  return 0;
}

inline
Uint32 Resource_limits::get_resource_free_shared(Uint32 id) const
{
  const Uint32 free_shared = m_shared - m_shared_in_use;
  require(id <= MM_RG_COUNT);
  const Resource_limit& rl = m_limit[id - 1];

  if (rl.m_prio_memory == Resource_limit::ULTRA_HIGH_PRIO_MEMORY)
  {
    return free_shared;
  }
  else if (rl.m_prio_memory == Resource_limit::HIGH_PRIO_MEMORY)
  {
    if (rl.m_curr <= rl.m_max_high_prio)
    {
      if (free_shared > m_ultra_prio_free_limit)
      {
        return (free_shared - m_ultra_prio_free_limit);
      }
      return 0;
    }
  }
  if (free_shared >= m_prio_free_limit)
  {
    return (free_shared - m_prio_free_limit);
  }
  return 0;
}

inline
Uint32 Resource_limits::get_resource_in_use(Uint32 id) const
{
  require(id <= MM_RG_COUNT);
  return m_limit[id - 1].m_curr;
}

inline
void Resource_limits::get_resource_limit(Uint32 id, Resource_limit& rl) const
{
  require(id <= MM_RG_COUNT);
  rl = m_limit[id - 1];
}

inline
Uint32 Resource_limits::get_resource_reserved(Uint32 id) const
{
  require(id > 0);
  require(id <= MM_RG_COUNT);
  return m_limit[id - 1].m_min;
}

inline
Uint32 Resource_limits::get_resource_spare(Uint32 id) const
{
  require(id > 0);
  require(id <= MM_RG_COUNT);
  return m_limit[id - 1].m_spare;
}

inline
void Resource_limits::inc_free_reserved(Uint32 cnt)
{
  m_free_reserved += cnt;
  assert(m_free_reserved >= cnt);
}

inline
void Resource_limits::inc_in_use(Uint32 cnt)
{
  m_in_use += cnt;
  assert(m_in_use >= cnt);
}

inline
Uint32
Resource_limits::get_stolen_reserved(Uint32 id) const
{
  return m_limit[id - 1].m_stolen_reserved;
}

inline
void
Resource_limits::inc_stolen_reserved(Uint32 id, Uint32 cnt)
{
  assert((m_limit[id - 1].m_stolen_reserved + cnt) < m_reserved);
  m_limit[id - 1].m_stolen_reserved += cnt;
}

inline
void
Resource_limits::dec_stolen_reserved(Uint32 id, Uint32 cnt)
{
  assert(m_limit[id - 1].m_stolen_reserved >= cnt);
  m_limit[id - 1].m_stolen_reserved -= cnt;
}

inline
Uint32
Resource_limits::get_overflow_reserved(Uint32 id) const
{
  return m_limit[id - 1].m_overflow_reserved;
}

inline
void
Resource_limits::inc_overflow_reserved(Uint32 id, Uint32 cnt)
{
  m_limit[id - 1].m_overflow_reserved += cnt;
}

inline
void
Resource_limits::dec_overflow_reserved(Uint32 id, Uint32 cnt)
{
  assert(m_limit[id - 1].m_overflow_reserved >= cnt);
  m_limit[id - 1].m_overflow_reserved -= cnt;
}

inline
void Resource_limits::inc_resource_in_use(Uint32 id, Uint32 cnt)
{
  m_limit[id - 1].m_curr += cnt;
  assert(m_limit[id - 1].m_curr >= cnt);
}

inline
void Resource_limits::post_release_resource_pages(Uint32 id, Uint32 cnt)
{
  Uint32 overflow_cnt = cnt;
  while (get_overflow_reserved(id) > 0 &&
         overflow_cnt > 0)
  {
    dec_overflow_reserved(id, 1);
    overflow_cnt--;
  }
  Uint32 stolen_cnt = cnt;
  while (get_stolen_reserved(id) > 0 &&
         stolen_cnt > 0)
  {
    inc_free_reserved(1);
    dec_stolen_reserved(id, 1);
    dec_resource_in_use(id, 1);
    dec_in_use(1);
    stolen_cnt--;
    if (stolen_cnt == 0)
    {
      return;
    }
  }
  /**
   * In the case when some pages were returned as stolen pages but not
   * all we need to handle the rest of with only the remaining pages
   * not handled by the stolen pages.
   */
  cnt = stolen_cnt;

  const Uint32 inuse = get_resource_in_use(id);
  const Uint32 reserved = get_resource_reserved(id) +
                          get_resource_spare(id);
  if ((inuse - cnt) < reserved)
  {
    Uint32 res_cnt = reserved - inuse + cnt;
    if (res_cnt >= cnt)
    {
      res_cnt = cnt;
    }
    else
    {
      /**
       * Parts of the memory will go into reserved memory and parts of it
       * will go into shared global memory.
       */
      Uint32 shared_cnt = cnt - res_cnt;
      dec_shared_in_use(shared_cnt);
    }
    inc_free_reserved(res_cnt);
  }
  else
  {
    /**
     * All of the reserved will still be used even after this release.
     * Thus simply decrease the amount of shared global memory used.
     */
    dec_shared_in_use(cnt);
  }
  dec_resource_in_use(id, cnt);
  dec_in_use(cnt);
}

inline
void Resource_limits::set_allocated(Uint32 cnt)
{
  m_allocated = cnt;
}

inline
void Resource_limits::set_shared()
{
  if (m_allocated > m_reserved)
  {
    m_shared = m_allocated - m_reserved;
  }
}

inline
void Resource_limits::set_prio_free_limits(Uint32 res)
{
  Uint64 shared = m_shared;
  Uint64 ultra_prio_free_limit = shared * Uint64(ULTRA_PRIO_FREE_PCT);
  ultra_prio_free_limit /= 100;
  ultra_prio_free_limit += 1;
  m_ultra_prio_free_limit = res + Uint32(ultra_prio_free_limit);

  Uint64 low_prio_free_limit = shared * Uint64(HIGH_PRIO_FREE_PCT);
  low_prio_free_limit /= 100;
  low_prio_free_limit += 1;
  m_prio_free_limit = Uint32(low_prio_free_limit) + m_ultra_prio_free_limit;

}

inline
void Resource_limits::set_max_page(Uint32 page)
{
  m_max_page = page;
}

/**
 * Ndbd_mem_manager
 */

inline
void*
Ndbd_mem_manager::get_page(Uint32 page_num) const
{
#ifdef NDBD_RANDOM_START_PAGE
  page_num -= m_random_start_page_id;
#endif
  return (void*)(m_base_page + page_num);
}
/**
 * get_valid_page returns page pointer if requested page is handled by
 * Ndbd_mem_manager, otherwise it returns NULL.
 *
 * Note: Use of function in release code paths should be regarded as bugs.
 * Accessing a page through a potentially invalid page reference is never a
 * good idea.
 *
 * This function is typically used for converting legacy code using static
 * arrays of records to dynamically allocated records.
 * For these static arrays there has been possible to inspect state of freed
 * records to determine that they are free.  Still this was a weak way to ensure
 * if the reference to record actually is to the right version of record.
 *
 * In some cases it is used to dump the all records of a kind for debugging
 * purposes, for these cases this function provides a mean to implement this
 * in a way to at least minimize risk for memory faults leading to program
 * exit.
 *
 * In any case one should strive to remove this function!
 */
inline
void*
Ndbd_mem_manager::get_valid_page(Uint32 page_num) const
{
#ifdef NDBD_RANDOM_START_PAGE
  page_num -= m_random_start_page_id;
#endif
  const Uint32 page_region_index = page_num & PAGE_REGION_MASK;
  if (unlikely(page_region_index == 0 ||
               page_region_index == PAGE_REGION_MASK))
  {
    /**
     * First page in region are used internally for bitmap.
     * Last page is region is reserved for no use.
     */
#ifdef NDBD_RANDOM_START_PAGE
    g_eventLogger->info(
        "Warning: Ndbd_mem_manager::get_valid_page: internal page %u %u",
        (page_num + m_random_start_page_id), page_num);
#else
    g_eventLogger->info(
        "Warning: Ndbd_mem_manager::get_valid_page: internal page %u",
        page_num);
#endif
#ifdef VM_TRACE
    abort();
#endif
    return NULL;
  }
  bool page_is_mapped;
  Uint32 lock_value;
  do {
    lock_value = m_mapped_pages_lock.read_lock();

    Uint32 a = 0; /* inclusive lower limit */
    Uint32 z = m_mapped_pages_count; /* exclusive upper limit */
    page_is_mapped = false;
    while (a < z)
    {
      Uint32 i = (a + z) / 2;
      if (page_num < m_mapped_pages[i].start)
      {
        z = i;
      }
      else if (page_num < m_mapped_pages[i].end)
      {
        page_is_mapped = true;
        break;
      }
      else
      {
        a = i + 1;
      }
    }
  } while (!m_mapped_pages_lock.read_unlock(lock_value));

  if (unlikely(!page_is_mapped))
  {
#ifdef NDBD_RANDOM_START_PAGE
    g_eventLogger->info(
        "Warning: Ndbd_mem_manager::get_valid_page: unmapped page %u %u",
        (page_num + m_random_start_page_id), page_num);
#else
    g_eventLogger->info(
        "Warning: Ndbd_mem_manager::get_valid_page: unmapped page %u",
        page_num);
#endif
#ifdef VM_TRACE
    abort();
#endif
    return NULL;
  }

  return (void*)(m_base_page + page_num);
}

inline
Free_page_data*
Ndbd_mem_manager::get_free_page_data(Alloc_page* ptr, Uint32 idx)
{
  assert(idx & ((1 << BPP_2LOG) - 1));
  assert((idx & ((1 << BPP_2LOG) - 1)) != ((1 << BPP_2LOG) - 1));
  
  return (Free_page_data*)
    (ptr->m_data + ((idx & ((BITMAP_WORDS >> FPD_2LOG) - 1)) << FPD_2LOG));
}

inline
Uint32 Ndbd_mem_manager::get_page_zone(Uint32 page)
{
  if (page < ZONE_19_BOUND)
  {
    return ZONE_19;
  }
  else if (page < ZONE_27_BOUND)
  {
    return ZONE_27;
  }
  else if (page < ZONE_30_BOUND)
  {
    return ZONE_30;
  }
  else
  {
    return ZONE_32;
  }
}

inline
void
Ndbd_mem_manager::set(Uint32 first, Uint32 last)
{
  /**
   * First and last page in a BPP region may not be available for external use.
   * First page is the bitmap page for the region.
   * Last page is always unused.
   */
  require(first & ((1 << BPP_2LOG) - 1));
  require((first+1) & ((1 << BPP_2LOG) - 1));
  Alloc_page * ptr = m_base_page;
#if ((SPACE_PER_BMP_2LOG < 32) && (SIZEOF_CHARP == 4)) || (SIZEOF_CHARP == 8)
  Uint32 bmp = first & ~((1 << BPP_2LOG) - 1);
  assert((first >> BPP_2LOG) == (last >> BPP_2LOG));
  assert(bmp < m_resource_limits.get_max_page());

  first -= bmp;
  last -= bmp;
  ptr += bmp;
#endif
  BitmaskImpl::set(BITMAP_WORDS, ptr->m_data, first);
  BitmaskImpl::set(BITMAP_WORDS, ptr->m_data, last);
}

inline
void
Ndbd_mem_manager::clear(Uint32 first, Uint32 last)
{
  Alloc_page * ptr = m_base_page;
#if ((SPACE_PER_BMP_2LOG < 32) && (SIZEOF_CHARP == 4)) || (SIZEOF_CHARP == 8)
  Uint32 bmp = first & ~((1 << BPP_2LOG) - 1);
  assert((first >> BPP_2LOG) == (last >> BPP_2LOG));
  assert(bmp < m_resource_limits.get_max_page());
  
  first -= bmp;
  last -= bmp;
  ptr += bmp;
#endif
  BitmaskImpl::clear(BITMAP_WORDS, ptr->m_data, first);
  BitmaskImpl::clear(BITMAP_WORDS, ptr->m_data, last);
}

inline
void
Ndbd_mem_manager::clear_and_set(Uint32 first, Uint32 last)
{
  Alloc_page * ptr = m_base_page;
#if ((SPACE_PER_BMP_2LOG < 32) && (SIZEOF_CHARP == 4)) || (SIZEOF_CHARP == 8)
  Uint32 bmp = first & ~((1 << BPP_2LOG) - 1);
  assert((first >> BPP_2LOG) == (last >> BPP_2LOG));
  assert(bmp < m_resource_limits.get_max_page());
  
  first -= bmp;
  last -= bmp;
  ptr += bmp;
#endif
  BitmaskImpl::clear(BITMAP_WORDS, ptr->m_data, first);
  BitmaskImpl::clear(BITMAP_WORDS, ptr->m_data, last);
  BitmaskImpl::set(BITMAP_WORDS, ptr->m_data, last+1);
}

inline
Uint32
Ndbd_mem_manager::check(Uint32 first, Uint32 last)
{
  Uint32 ret = 0;
  Alloc_page * ptr = m_base_page;
#if ((SPACE_PER_BMP_2LOG < 32) && (SIZEOF_CHARP == 4)) || (SIZEOF_CHARP == 8)
  Uint32 bmp = first & ~((1 << BPP_2LOG) - 1);
  assert((first >> BPP_2LOG) == (last >> BPP_2LOG));
  assert(bmp < m_resource_limits.get_max_page());
  
  first -= bmp;
  last -= bmp;
  ptr += bmp;
#endif
  ret |= BitmaskImpl::get(BITMAP_WORDS, ptr->m_data, first) << 0;
  ret |= BitmaskImpl::get(BITMAP_WORDS, ptr->m_data, last) << 1;
  return ret;
}

#undef JAM_FILE_ID

#endif 
