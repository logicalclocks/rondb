/*
   Copyright (c) 2005, 2022, Oracle and/or its affiliates.
   Copyright (c) 2021, 2022, Hopsworks and/or its affiliates.

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

#define DBTUP_C
#define DBTUP_DISK_ALLOC_CPP
#include "Dbtup.hpp"
#include <signaldata/LgmanContinueB.hpp>
#include "../dblqh/Dblqh.hpp"

#define JAM_FILE_ID 426

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_UNDO_SPLIT 1
//#define DEBUG_LCP 1
//#define DEBUG_PGMAN_IO 1
//#define DEBUG_PGMAN 1
//#define DEBUG_EXTENT_BITS 1
//#define DEBUG_EXTENT_BITS_HASH 1
//#define DEBUG_FREE_EXTENT 1
//#define DEBUG_UNDO 1
//#define DEBUG_UNDO_LCP 1
//#define DEBUG_UNDO_ALLOC 1
//#define DEBUG_FREE_SPACE 1
#endif

#ifdef DEBUG_FREE_SPACE
#define DEB_FREE_SPACE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FREE_SPACE(arglist) do { } while (0)
#endif

#ifdef DEBUG_LCP
#define DEB_LCP(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_LCP(arglist) do { } while (0)
#endif

#ifdef DEBUG_PGMAN
#define DEB_PGMAN(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_PGMAN(arglist) do { } while (0)
#endif

#ifdef DEBUG_PGMAN_IO
#define DEB_PGMAN_IO(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_PGMAN_IO(arglist) do { } while (0)
#endif

#ifdef DEBUG_EXTENT_BITS
#define DEB_EXTENT_BITS(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_EXTENT_BITS(arglist) do { } while (0)
#endif

#ifdef DEBUG_EXTENT_BITS_HASH
#define DEB_EXTENT_BITS_HASH(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_EXTENT_BITS_HASH(arglist) do { } while (0)
#endif

#ifdef DEBUG_FREE_EXTENT
#define DEB_FREE_EXTENT(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_FREE_EXTENT(arglist) do { } while (0)
#endif

#ifdef DEBUG_UNDO
#define DEB_UNDO(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_UNDO(arglist) do { } while (0)
#endif

#ifdef DEBUG_UNDO_LCP
#define DEB_UNDO_LCP(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_UNDO_LCP(arglist) do { } while (0)
#endif

#ifdef DEBUG_UNDO_ALLOC
#define DEB_UNDO_ALLOC(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_UNDO_ALLOC(arglist) do { } while (0)
#endif

#ifdef DEBUG_UNDO_SPLIT
static void
log_buf_print(const Uint32 *ptr, Uint32 len)
{
  while (len >= 8)
  {
    g_eventLogger->info("%x %x %x %x %x %x %x %x",
                        ptr[0],
                        ptr[1],
                        ptr[2],
                        ptr[3],
                        ptr[4],
                        ptr[5],
                        ptr[6],
                        ptr[7]);
    ptr+= 8;
    len -= 8;
  }
  if (len >= 4)
  {
    g_eventLogger->info("%x %x %x %x",
                        ptr[0],
                        ptr[1],
                        ptr[2],
                        ptr[3]);
    ptr+= 4;
    len -= 4;
  }
  if (len >= 2)
  {
    g_eventLogger->info("%x %x",
                        ptr[0],
                        ptr[1]);
    ptr+= 2;
    len -= 2;
  }
  if (len > 0)
  {
    g_eventLogger->info("%x",
                        ptr[0]);
  }
}
#endif

void
Dbtup::printPtr(EventLogger *logger, int idx, const Ptr<Dbtup::Page> &ptr)
{
  logger->info(
      "Dirty_pages %d [ Page: ptr.i: %u [ m_m_page_lsn_hi: %u"
      " m_m_page_lsn_lo: %u m_page_type: %u m_file_no: %u m_page_no: %u"
      " m_table_id: %u m_fragment_id: %u m_extent_no: %u m_extent_info_ptr: %u"
      " m_restart_seq: %u] list_index: %u free_space: %u"
      " uncommitted_used_space: %u ] ",
      idx, ptr.i, ptr.p->m_page_header.m_page_lsn_hi,
      ptr.p->m_page_header.m_page_lsn_lo, ptr.p->m_page_header.m_page_type,
      ptr.p->m_file_no, ptr.p->m_page_no, ptr.p->m_table_id,
      ptr.p->m_fragment_id, ptr.p->m_extent_no, ptr.p->m_extent_info_ptr,
      ptr.p->m_restart_seq, ptr.p->list_index, ptr.p->free_space,
      ptr.p->uncommitted_used_space);
}

void
Dbtup::printPtr(EventLogger *logger, int idx,
                const Ptr<Dbtup::Page_request> &ptr)
{
  char buf[MAX_LOG_MESSAGE_SIZE];
  logger->info(
      "Page requests %d [ Page_request: ptr.i: %u %s"
      " m_original_estimated_free_space: %u"
      " m_list_index: %u"
      " m_frag_ptr_i: %llu"
      " m_extent_info_ptr: %u"
      " m_ref_count: %u"
      " m_uncommitted_used_space: %u"
      " ] ",
      idx, ptr.i, printLocal_Key(buf, MAX_LOG_MESSAGE_SIZE, ptr.p->m_key),
      ptr.p->m_original_estimated_free_space, ptr.p->m_list_index,
      ptr.p->m_frag_ptr_i, ptr.p->m_extent_info_ptr, ptr.p->m_ref_count,
      ptr.p->m_uncommitted_used_space);
}

void
Dbtup::printPtr(EventLogger *logger, const char *msg, int idx,
                const Ptr<Dbtup::Extent_info> &ptr)
{
  char buf[MAX_LOG_MESSAGE_SIZE];
  g_eventLogger->info(
      "%s %d [ Extent_info: ptr.i %u %s"
      " m_first_page_no: %u"
      " m_empty_page_no: %u"
      " m_key: ["
      " m_file_no=%u"
      " m_first_page_no=%u"
      " ]"
      " m_extent_no: %u"
      " m_free_space: %u"
      " m_free_matrix_pos: %u"
      " m_free_page_count: [",
      msg, idx, ptr.i, printLocal_Key(buf, MAX_LOG_MESSAGE_SIZE, ptr.p->m_key),
      ptr.p->m_first_page_no, ptr.p->m_empty_page_no, ptr.p->m_key.m_file_no,
      ptr.p->m_key.m_page_no, ptr.p->m_extent_no, ptr.p->m_free_space,
      ptr.p->m_free_matrix_pos);
}

void 
Dbtup::dump_disk_alloc(Dbtup::Disk_alloc_info & alloc)
{
  const Uint32 limit = 512;
  for(Uint32 i = 0; i < EXTENT_SEARCH_MATRIX_COLS; i++)
  {
    PagePtr ptr;
    Page_pool *pool= (Page_pool*)&m_global_page_pool;
    Local_Page_list list(*pool, alloc.m_dirty_pages[i]);
    Uint32 c = 0;
    bool empty = true;
    for (list.first(ptr); c < limit && !ptr.isNull(); c++, list.next(ptr))
    {
      empty = false;
      printPtr(g_eventLogger, i, ptr);
    }
    if (empty)
    {
      g_eventLogger->info("Dirty pages: %d EMPTY", i);
    }
    if (c == limit)
    {
      g_eventLogger->info("Dirty pages: %d MAXLIMIT", i);
    }
  }
  for(Uint32 i = 0; i < EXTENT_SEARCH_MATRIX_COLS; i++)
  {
    Ptr<Page_request> ptr;
    Local_page_request_list list(c_page_request_pool, 
				 alloc.m_page_requests[i]);
    Uint32 c = 0;
    bool empty = true;
    for (list.first(ptr); c < limit && !ptr.isNull(); c++, list.next(ptr))
    {
      empty = false;
      printPtr(g_eventLogger, i, ptr);
    }
    if (empty)
    {
      g_eventLogger->info("Page requests: %d EMPTY", i);
    }
    if (c == limit)
    {
      g_eventLogger->info("Page requests: %d MAXLIMIT", i);
    }
  }

  for(Uint32 i = 0; i<alloc.SZ; i++)
  {
    Ptr<Extent_info> ptr;
    Local_extent_info_list list(c_extent_pool, alloc.m_free_extents[i]);
    Uint32 c = 0;
    bool empty = true;
    for (list.first(ptr); c < limit && !ptr.isNull(); c++, list.next(ptr))
    {
      empty = false;
      printPtr(g_eventLogger, "Extent matrix: ", i, ptr);
    }
    if (empty)
    {
      g_eventLogger->info("Extent matrix: %d EMPTY", i);
    }
    if (c == limit)
    {
      g_eventLogger->info("Extent matrix: %d MAXLIMIT", i);
    }
  }

  if (alloc.m_curr_extent_info_ptr_i != RNIL)
  {
    Ptr<Extent_info> ptr;
    ndbrequire(c_extent_pool.getPtr(ptr, alloc.m_curr_extent_info_ptr_i));
    printPtr(g_eventLogger, "Current extent: ", 0, ptr);
  }
}

#define ddrequire(x) do { if(unlikely(!(x))) { dump_disk_alloc(alloc); ndbabort(); } } while(0)
#if defined(VM_TRACE) || defined(ERROR_INSERT)
#define ddassert(x) do { if(unlikely(!(x))) { dump_disk_alloc(alloc); ndbabort(); } } while(0)
#else
#define ddassert(x)
#endif

Dbtup::Disk_alloc_info::Disk_alloc_info(const Tablerec* tabPtrP, 
					Uint32 extent_size)
{
  m_extent_size = extent_size;
  m_curr_extent_info_ptr_i = RNIL;
  m_tot_free_space = 0;
  if (tabPtrP->m_no_of_disk_attributes == 0)
    return;
  
  if ((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    /* We set values according to the min_size. */
    Uint32 min_size = tabPtrP->m_offsets[DD].m_fix_header_size;
    Uint32 recs_per_page= Tup_fixsize_page::DATA_WORDS / min_size;
    m_page_free_bits_map[0] = recs_per_page; // 100% free
    m_page_free_bits_map[1] = 1;
    m_page_free_bits_map[2] = 0;
    m_page_free_bits_map[3] = 0;
    
    Uint32 max= recs_per_page * extent_size;
    for(Uint32 i = 0; i<EXTENT_SEARCH_MATRIX_ROWS; i++)
    {
      m_total_extent_free_space_thresholds[i] = 
	(EXTENT_SEARCH_MATRIX_ROWS - i - 1)*max/EXTENT_SEARCH_MATRIX_ROWS;
    }
  }
  else
  {
    /**
     * The value in 0 is a full page, the value in 3 is 0 since it
     * represents an empty page.
     *
     * The value 1 represents half-full, this is set to half of a full
     * page independent of the size of the maximum disk record size.
     * The reason is that the bits in the extent are not affected by
     * added or dropped columns. The variable sized structure makes
     * it possible to add columns such that the original maximum record
     * size is greatly enhanced. Thus we want the limits to be stable
     * over time.
     *
     * The value 2 represents almost full, this is set to a third of
     * the value 1.
     */
    m_page_free_bits_map[0] = Tup_fixsize_page::DATA_WORDS - 1;
    m_page_free_bits_map[1] = Tup_fixsize_page::DATA_WORDS / 2;
    m_page_free_bits_map[2] = Tup_fixsize_page::DATA_WORDS / 6;
    m_page_free_bits_map[3] = 0;

    Uint32 max = (Tup_fixsize_page::DATA_WORDS - 1) * extent_size;
    for(Uint32 i = 0; i<EXTENT_SEARCH_MATRIX_ROWS; i++)
    {
      m_total_extent_free_space_thresholds[i] = 
	(EXTENT_SEARCH_MATRIX_ROWS - i - 1)*max/EXTENT_SEARCH_MATRIX_ROWS;
    }
  }
}

Uint32
Dbtup::Disk_alloc_info::find_extent(Uint32 sz) const
{
  /**
   * Find an extent with sufficient space for sz
   * Find the biggest available (with most free space)
   * Return position in matrix
   */
  Uint32 col = calc_page_free_bits(sz);
  Uint32 mask= EXTENT_SEARCH_MATRIX_COLS - 1;
  if (m_page_free_bits_map[2] > 0)
  {
    /**
     * The column returned from calc_page_free_bits can't promise
     * what is required, we need to step back one step
     * returning this as an extent that can find any
     * free space suitable.
     */
    require(col > 0);
    col--;
  }
  if (col == mask)
  {
  }
  for(Uint32 i= 0; i<EXTENT_SEARCH_MATRIX_SIZE; i++)
  {
    // Check that it can cater for request
    if (!m_free_extents[i].isEmpty())
    {
      return i;
    }
    
    if ((i & mask) >= col)
    {
      i = (i & ~mask) + mask;
    }
  }
  
  return RNIL;
}

Uint32
Dbtup::Disk_alloc_info::calc_extent_pos(const Extent_info* extP) const
{
  Uint32 free= extP->m_free_space;
  Uint32 mask= EXTENT_SEARCH_MATRIX_COLS - 1;
  
  Uint32 col= 0, row=0;
  
  /**
   * Find correct row based on total free space
   *   if zero (or very small free space) put 
   *     absolutely last
   */
  {    
    while (free < m_total_extent_free_space_thresholds[row])
    {
      row++;
    }
    assert(row < EXTENT_SEARCH_MATRIX_ROWS);
    DEB_FREE_EXTENT(("(%p) free: %u, row: %u",
                     this,
                     free,
                     row));
  }

  /**
   * Find correct col based on largest available chunk
   */
  {
    while (col < EXTENT_SEARCH_MATRIX_COLS &&
           extP->m_free_page_count[col] == 0)
    {
      col++;
    }
  }

  /**
   * NOTE
   *
   *   If free space on extent is small or zero,
   *     col will be = EXTENT_SEARCH_MATRIX_COLS
   *     row will be = EXTENT_SEARCH_MATRIX_ROWS
   *   in that case pos will be col * row = max pos
   *   (as fixed by + 1 in declaration)
   */
  Uint32 pos= (row * (mask + 1)) + (col & mask);
  DEB_FREE_EXTENT(("(%p) col: %u, pos: %u", this, col, pos));
  
  assert(pos < EXTENT_SEARCH_MATRIX_SIZE);
  return pos;
}

void
Dbtup::update_extent_pos(EmulatedJamBuffer* jamBuf,
                         Fragrecord *fragPtrP,
                         Ptr<Extent_info> extentPtr,
                         Uint32 before_bits,
                         Uint32 after_bits,
                         Int32 delta)
{
  Disk_alloc_info& alloc = fragPtrP->m_disk_alloc_info;
  if (alloc.m_page_free_bits_map[1] == 1)
  {
    /* Old method with fixed size disk rows */
  }
  else
  {
    Uint32 before_size = alloc.calc_page_free_space(before_bits);
    Uint32 after_size = alloc.calc_page_free_space(after_bits);
    if (after_size == before_size && delta != 0)
    {
      return;
    }
    delta = after_size - before_size;
  }
  if (delta < 0)
  {
    thrjam(jamBuf);
    Uint32 sub = Uint32(- delta);
    ddrequire(extentPtr.p->m_free_space >= sub);
    extentPtr.p->m_free_space -= sub;
    DEB_FREE_SPACE(("(%u)1 Decrease %u m_free_space: %u, extP: %p",
                    instance(),
                    sub,
                    extentPtr.p->m_free_space,
                    extentPtr.p));
    ddrequire(alloc.m_tot_free_space >= sub);
    alloc.m_tot_free_space -= sub;
  }
  else
  {
    thrjam(jamBuf);
    extentPtr.p->m_free_space += delta;
    DEB_FREE_SPACE(("(%u)2 Increase %u m_free_space: %u, extP: %p",
                    instance(),
                    delta,
                    extentPtr.p->m_free_space,
                    extentPtr.p));
    ndbassert(Uint32(delta) <= alloc.calc_page_free_space(0));
    alloc.m_tot_free_space += delta;
  }

#if defined(VM_TRACE) || defined(ERROR_INSERT)
  Uint32 cnt = 0;
  Uint32 sum = 0;
  for(Uint32 i = 0; i < EXTENT_SEARCH_MATRIX_COLS; i++)
  {
    cnt += extentPtr.p->m_free_page_count[i];
    sum += extentPtr.p->m_free_page_count[i] * alloc.calc_page_free_space(i);
  }

  DEB_FREE_SPACE(("(%u) cnt: %u, sum: %u, extentPtrP: %p",
                  instance(),
                  cnt,
                  sum,
                  extentPtr.p));

  if (extentPtr.p->m_free_page_count[0] == cnt)
  {
    ddrequire(extentPtr.p->m_free_space == cnt*alloc.m_page_free_bits_map[0]);
  }
  else
  {
    ddrequire(extentPtr.p->m_free_space < cnt*alloc.m_page_free_bits_map[0]);
  }
  ddrequire(extentPtr.p->m_free_space >= sum);
  ddrequire(extentPtr.p->m_free_space <= cnt*alloc.m_page_free_bits_map[0]);
#endif
  
  Uint32 old = extentPtr.p->m_free_matrix_pos;
  if (old != RNIL)
  {
    thrjam(jamBuf);
    Uint32 pos = alloc.calc_extent_pos(extentPtr.p);
    if (old != pos)
    {
      thrjam(jamBuf);
      Local_extent_info_list old_list(c_extent_pool,
                                      alloc.m_free_extents[old]);
      Local_extent_info_list new_list(c_extent_pool,
                                      alloc.m_free_extents[pos]);
      old_list.remove(extentPtr);
      new_list.addFirst(extentPtr);
      DEB_FREE_EXTENT(("(%p) 2:Add/Remove ext.p: %p from old pos: %u"
                       " to pos: %u",
                       &alloc, extentPtr.p, old, pos));
      extentPtr.p->m_free_matrix_pos= pos;
    }
  }
  else
  {
    ddrequire(alloc.m_curr_extent_info_ptr_i == extentPtr.i);
  }
}

void
Dbtup::restart_setup_page(Fragrecord *fragPtrP,
                          Disk_alloc_info& alloc,
                          PagePtr pagePtr,
                          Int32 estimate)
{
  jam();
  /**
   * Link to extent, clear uncommitted_used_space
   */
  pagePtr.p->uncommitted_used_space = 0;
  
  Extent_info key;
  key.m_key.m_file_no = pagePtr.p->m_file_no;
  key.m_extent_no = pagePtr.p->m_extent_no;
  Ptr<Extent_info> extentPtr;
  if (!c_extent_hash.find(extentPtr, key))
  {
    g_eventLogger->info("(%u)Crash on page(%u,%u) in tab(%u,%u),"
                        " extent page: %u"
                        " restart_seq(%u,%u)",
                        instance(),
                        pagePtr.p->m_file_no,
                        pagePtr.p->m_page_no,
                        fragPtrP->fragTableId,
                        fragPtrP->fragmentId,
                        pagePtr.p->m_extent_no,
                        pagePtr.p->m_restart_seq,
                        globalData.m_restart_seq);
    ndbabort();
  }
  DEB_EXTENT_BITS(("(%u)restart_setup_page(%u,%u) in tab(%u,%u),"
                   " extent page: %u.%u"
                   " restart_seq(%u,%u), free_space: %u, idx: %u"
                   ", pagePtr.i = %u",
                   instance(),
                   pagePtr.p->m_file_no,
                   pagePtr.p->m_page_no,
                   fragPtrP->fragTableId,
                   fragPtrP->fragmentId,
                   pagePtr.p->m_extent_no,
                   extentPtr.i,
                   pagePtr.p->m_restart_seq,
                   globalData.m_restart_seq,
                   pagePtr.p->free_space,
                   pagePtr.p->list_index,
                   pagePtr.i));

  pagePtr.p->m_restart_seq = globalData.m_restart_seq;
  pagePtr.p->m_extent_info_ptr = extentPtr.i;

  Uint32 real_free = pagePtr.p->free_space;
  Uint32 before_bits = alloc.calc_page_free_bits(real_free);
  Uint32 after_bits = before_bits;
  const bool prealloc = estimate >= 0;
  Uint32 estimated;
  if (prealloc)
  {
    jam();
    /**
     * If this is during prealloc, use estimate from there
     */
    estimated = (Uint32)estimate;
    after_bits = alloc.calc_page_free_bits(estimated);
    Uint32 page_estimated =
      alloc.calc_page_free_space(alloc.calc_page_free_bits(real_free));
    if (page_estimated != estimated && real_free == 0)
    {
      jam();
      /**
       * The page claims it is full, but the extent bits says that it isn't
       * full, this can occur if the tablespace is using the v1 page format.
       * It must be an old dropped page and thus we can safely overwrite it.
       */
      g_eventLogger->info("(%u)tab(%u,%u), page(%u,%u):%u"
                          ", inconsistency between extent and page, most"
                          " likely due to using v1 pages, we assume page"
                          " comes from dropped table and is really empty",
                          instance(),
                          fragPtrP->fragTableId,
                          fragPtrP->fragmentId,
                          pagePtr.p->m_file_no,
                          pagePtr.p->m_page_no,
                          pagePtr.i);
      ndbassert(false); //Crash in debug for analysis
      Ptr<Tablerec> tabPtr;
      tabPtr.i= fragPtrP->fragTableId;
      ptrCheckGuard(tabPtr, cnoOfTablerec, tablerec);
      convertThPage((Fix_page*)pagePtr.p, tabPtr.p, DD);
      estimated = alloc.calc_page_free_space(
        alloc.calc_page_free_bits(real_free));
    }
  }
  else
  {
    jam();
    /**
     * else use the estimate based on the actual free space
     */
    estimated =
      alloc.calc_page_free_space(alloc.calc_page_free_bits(real_free));
  }

#if defined(VM_TRACE) || defined(ERROR_INSERT)
  {
    Local_key page;
    page.m_file_no = pagePtr.p->m_file_no;
    page.m_page_no = pagePtr.p->m_page_no;

    D("Tablespace_client - restart_setup_page");
    Tablespace_client tsman(0, this, c_tsman,
			    0, 0, 0, 0);
    unsigned uncommitted, committed;
    uncommitted = committed = ~(unsigned)0;
    (void) tsman.get_page_free_bits(&page, &uncommitted, &committed);
    jamEntry();

    if (alloc.calc_page_free_bits(real_free) != committed)
    {
      Uint64 page_lsn = 0;
      page_lsn += pagePtr.p->m_page_header.m_page_lsn_hi;
      page_lsn <<= 32;
      page_lsn += pagePtr.p->m_page_header.m_page_lsn_lo;
      g_eventLogger->info("(%u)page(%u,%u):%u, calc_free_bits: %u,"
                          " committed: %u, uncommitted: %u, free_space: %u"
                          ", page_lsn: %llu",
                          instance(),
                          page.m_file_no,
                          page.m_page_no,
                          pagePtr.i,
                          alloc.calc_page_free_bits(real_free),
                          committed,
                          uncommitted,
                          real_free,
                          page_lsn);
    }
    ddassert(alloc.calc_page_free_bits(real_free) == committed);
    if (prealloc)
    {
      /**
       * tsman.alloc_page sets the uncommitted-bits to EXTENT_SEARCH_MATRIX_COLS -1
       *   to avoid page being preallocated several times
       */
      ddassert(uncommitted == EXTENT_SEARCH_MATRIX_COLS - 1);
    }
    else
    {
      ddassert(committed == uncommitted);
    }
  }
#endif

  ddrequire(real_free >= estimated);

  if (real_free != estimated)
  {
    jam();
    Uint32 delta = (real_free-estimated);
    DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)",
                    instance(),
                    __LINE__,
                    pagePtr.p->m_file_no,
                    pagePtr.p->m_page_no));
    update_extent_pos(jamBuffer(),
                      fragPtrP,
                      extentPtr,
                      before_bits,
                      after_bits,
                      delta);
  }
}

/**
 * - Page free bits -
 * 0 = 00 - free - 100% free
 * 1 = 01 - at least one row free
 * 2 = 10 - full
 * 3 = 11 - full
 *
 * sz is always 1 when coming here, so calc_page_free_bits will
 * will always return 1 here. This will change with implementation
 * var-sized disk attributes.
 */

#define DBG_DISK 0

int
Dbtup::disk_page_prealloc(Signal* signal, 
                          FragrecordPtr fragPtr,
                          Tablerec *regTabPtr,
			  Local_key* key,
                          Uint32 sz)
{
  int err;
  Uint32 i, ptrI;
  Ptr<Page_request> req;
  Fragrecord* fragPtrP = fragPtr.p; 
  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;
  Uint32 idx;
  if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    jam();
    /**
     * For fixed size rows the sz == 1 and thus we will always end up in
     * slot 1.
     */
    idx = alloc.calc_page_free_bits(sz);
  }
  else
  {
    /**
     * The calc_page_free_bits returns the slot where a page with this
     * free space would end up. However we need the slot that makes
     * it certain that we find the required size. For example a slot of
     * size 100 would end up in slot 3. However a page in this slot
     * guarantees no size at all. Thus we have to move to slot 2 which
     * do definitely guarantee that size 100 will be found.
     *
     * If we come here with a size which is exactly the size of the
     * limit of a slot, it will return an index that is ok, but we
     * will increase it one level which isn't necessary. Thus we
     * decrease the value by 1 to ensure that we end up in the right
     * slot also when equal to the slot limits.
     */
    jam();
    idx = alloc.calc_page_free_bits(sz - 1);
    ndbrequire(idx > 0);
    idx--;
  }
  D("Tablespace_client - disk_page_prealloc");
  
  /**
   * 1) search current dirty pages
   * First check for empty pages and then search for non-full pages.
   */
  for(i= 0; i <= idx; i++)
  {
    if (!alloc.m_dirty_pages[i].isEmpty())
    {
      jam();
      jamLine(i);
      ptrI= alloc.m_dirty_pages[i].getFirst();
      Ptr<GlobalPage> gpage;
      ndbrequire(m_global_page_pool.getPtr(gpage, ptrI));
      
      PagePtr tmp;
      tmp.i = gpage.i;
      tmp.p = reinterpret_cast<Page*>(gpage.p);
      disk_page_prealloc_dirty_page(alloc, tmp, i, sz, fragPtrP);
      key->m_page_no= tmp.p->m_page_no;
      key->m_file_no= tmp.p->m_file_no;
      jam();
      return 0; // Page in memory
    }
  }
  
  /**
   * Search outanding page requests
   *   callback does not need to access page request again
   *   as it's not the first request to this page
   */
  for(i= 0; i <= idx; i++)
  {
    if (!alloc.m_page_requests[i].isEmpty())
    {
      jam();
      jamLine(i);
      ptrI= alloc.m_page_requests[i].getFirst();
      Ptr<Page_request> req;
      ndbrequire(c_page_request_pool.getPtr(req, ptrI));

      disk_page_prealloc_transit_page(fragPtr.p, req, i, sz);
      * key = req.p->m_key;
      jam();
      return 0;
    }
  }
  
  /**
   * We need to request a page...
   */
  if (!c_page_request_pool.seize(req))
  {
    jam();
    err= 1605;
    return -err;
  }

  req.p->m_ref_count= 1;
  req.p->m_frag_ptr_i= fragPtr.i;
  req.p->m_uncommitted_used_space= sz;
  
  int pageBits = 0; // received
  Ptr<Extent_info> ext;
  const Uint32 bits = idx;
  bool found= false;

  /**
   * Do we have a current extent
   */
  if ((ext.i = alloc.m_curr_extent_info_ptr_i) != RNIL)
  {
    jam();
    {
      Tablespace_client tsman(signal, this, c_tsman,
                    fragPtrP->fragTableId,
                    fragPtrP->fragmentId,
                    c_lqh->getCreateSchemaVersion(fragPtrP->fragTableId),
                    fragPtrP->m_tablespace_id);
      c_extent_pool.getPtr(ext);
      pageBits= tsman.alloc_page_from_extent(&ext.p->m_key, bits);
    }
    if (pageBits >= 0) 
    {
      jamEntry();
      jamLine(pageBits);
      found= true;
    }
    else
    {
      jamEntry();
      /**
       * The current extent is not in a free list
       *   and since it couldn't accommodate the request
       *   we put it on the free list per state (so also
       *   a full page is in one of the m_free_extents
       *   lists).
       */
      alloc.m_curr_extent_info_ptr_i = RNIL;
      Uint32 pos= alloc.calc_extent_pos(ext.p);
      ext.p->m_free_matrix_pos = pos;
      Local_extent_info_list list(c_extent_pool, alloc.m_free_extents[pos]);
      list.addFirst(ext);
      DEB_FREE_EXTENT(("(%p) Add ext.p: %p to pos: %u",
                       &alloc, ext.p, pos));
    }
  }
  
  if (!found)
  {
    Uint32 pos;
    if ((pos= alloc.find_extent(sz)) != RNIL)
    {
      jam();
      Local_extent_info_list list(c_extent_pool, alloc.m_free_extents[pos]);
      list.first(ext);
      list.remove(ext);
      DEB_FREE_EXTENT(("(%p) Remove ext.p: %p from pos: %u",
                       &alloc, ext.p, pos));
    }
    else 
    {
      jam();
      /**
       * We need to alloc an extent
       */
      if (!c_extent_pool.seize(ext))
      {
	jam();
	err= 1606;
	c_page_request_pool.release(req);
	return -err;
      }
      {  
        Tablespace_client tsman(signal, this, c_tsman,
                      fragPtrP->fragTableId,
                      fragPtrP->fragmentId,
                      c_lqh->getCreateSchemaVersion(fragPtrP->fragTableId),
                      fragPtrP->m_tablespace_id);
        err= tsman.alloc_extent(&ext.p->m_key, &ext.p->m_extent_no);
      }
      if (err < 0)
      {
        jamEntry();
        c_extent_pool.release(ext);
        c_page_request_pool.release(req);
        return err;
      }

      int pages= err;
      
#ifdef VM_TRACE
      ndbout << "allocated " << pages << " pages: " << ext.p->m_key 
	     << " table: " << fragPtr.p->fragTableId 
	     << " fragment: " << fragPtr.p->fragmentId << endl;
#endif
      ext.p->m_first_page_no = ext.p->m_key.m_page_no;
      memset(ext.p->m_free_page_count, 0, sizeof(ext.p->m_free_page_count));
      ext.p->m_free_space= alloc.m_page_free_bits_map[0] * pages;
      DEB_FREE_SPACE(("(%u)3 Set m_free_space: %u, extP: %p",
                      instance(),
                      ext.p->m_free_space,
                      ext.p));
      alloc.m_tot_free_space += ext.p->m_free_space;
      ext.p->m_free_page_count[0]= pages; // All pages are "free"-est
      ext.p->m_empty_page_no = 0;

      DEB_EXTENT_BITS_HASH((
               "(%u)new:extent .i=%u in tab(%u,%u),"
               " page(%u,%u)->%u,"
               " empty_page: %u",
                instance(),
                ext.i,
                fragPtr.p->fragTableId,
                fragPtr.p->fragmentId,
                ext.p->m_key.m_file_no,
                ext.p->m_first_page_no,
                ext.p->m_first_page_no + (pages - 1),
                ext.p->m_empty_page_no));

      c_extent_hash.add(ext);

      Local_fragment_extent_list list1(c_extent_pool, alloc.m_extent_list);
      list1.addFirst(ext);
    }
    jam(); 
    alloc.m_curr_extent_info_ptr_i= ext.i;
    ext.p->m_free_matrix_pos= RNIL;
    {
      Tablespace_client tsman(signal, this, c_tsman,
                    fragPtrP->fragTableId,
                    fragPtrP->fragmentId,
                    c_lqh->getCreateSchemaVersion(fragPtrP->fragTableId),
                    fragPtrP->m_tablespace_id);
      pageBits= tsman.alloc_page_from_extent(&ext.p->m_key, bits);
    }
    jamEntry();
    ddrequire(pageBits >= 0);
  }
  
  /**
   * We have a page from an extent
   */
  *key= req.p->m_key= ext.p->m_key;

  /**
   * We don't know exact free space of page
   *   but we know what page free bits it has.
   *   compute free space based on them
   */
  Uint32 size= alloc.calc_page_free_space((Uint32)pageBits);
  
  ddrequire(size >= sz);
  req.p->m_original_estimated_free_space = size;

  Uint32 new_size = size - sz;   // Subtract alloc rec
  Uint32 newPageBits= alloc.calc_page_free_bits(new_size);
  ndbrequire(newPageBits != (Uint32)pageBits)
  {
    jam();
    /**
     * We should always enter this path. When the new page was empty
     * before coming here, then it will go from empty state to either
     * non-full or to the full state. If we come here with a page which
     * non-full before, then we will enter the full state. We will
     * possibly return it to the non-full list when the real page have
     * been read and we know the exact fullness level.
     */
    DEB_EXTENT_BITS(("(%u)alloc page, extent(%u), pageBits: %u,"
                     " newPageBits: %u, free_page_count(%u,%u)",
                     instance(),
                     ext.p->m_extent_no,
                     pageBits,
                     newPageBits,
                     ext.p->m_free_page_count[pageBits],
                     ext.p->m_free_page_count[newPageBits]));
    ddrequire(ext.p->m_free_page_count[pageBits] > 0);
    ext.p->m_free_page_count[pageBits]--;
    ext.p->m_free_page_count[newPageBits]++;

  }
  DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)",
                  instance(),
                  __LINE__,
                  key->m_file_no,
                  key->m_page_no));
  update_extent_pos(jamBuffer(),
                    fragPtr.p,
                    ext,
                    pageBits,
                    newPageBits,
                    -Int32(sz));

  // And put page request in correct free list
  Uint32 new_idx = alloc.calc_page_free_bits(new_size);
  jamLine(new_idx);
  {
    Local_page_request_list list(c_page_request_pool, 
				 alloc.m_page_requests[new_idx]);
    
    list.addLast(req);
  }
  req.p->m_list_index= new_idx;
  req.p->m_extent_info_ptr= ext.i;

  Page_cache_client::Request preq;
  preq.m_page = *key;
  preq.m_table_id = fragPtr.p->fragTableId;
  preq.m_fragment_id = fragPtr.p->fragmentId;
  preq.m_callback.m_callbackData= req.i;
  preq.m_callback.m_callbackFunction = 
    safe_cast(&Dbtup::disk_page_prealloc_callback);
  
  int flags= Page_cache_client::ALLOC_REQ;
  if (pageBits == 0)
  {
    jam();
    flags |= Page_cache_client::EMPTY_PAGE;
    if (ext.p->m_first_page_no + ext.p->m_empty_page_no == key->m_page_no)
    {
      jam();
      ext.p->m_empty_page_no++;
      DEB_EXTENT_BITS(("(%u)extent(%u) new page in tab(%u,%u), first_page(%u,%u)"
                       " empty_page: %u",
                instance(),
                ext.p->m_extent_no,
                fragPtr.p->fragTableId,
                fragPtr.p->fragmentId,
                key->m_file_no,
                key->m_page_no,
                ext.p->m_empty_page_no));
    }
    else
    {
      DEB_EXTENT_BITS(("(%u)extent(%u) new page in tab(%u,%u), page(%u,%u)",
                instance(),
                ext.p->m_extent_no,
                fragPtr.p->fragTableId,
                fragPtr.p->fragmentId,
                key->m_file_no,
                key->m_page_no));
    }
    preq.m_callback.m_callbackFunction = 
      safe_cast(&Dbtup::disk_page_prealloc_initial_callback);
  }
  
  Page_cache_client pgman(this, c_pgman);
  int res= pgman.get_page(signal, preq, flags);
  jamEntry();
  switch(res)
  {
  case 0:
    jam();
    break;
  case -1:
    return -1604;
  case -1518:
    return -res;
  default:
    ndbrequire(res > 0);
    jam();
    execute(signal, preq.m_callback, res); // run callback
  }
  
  return res;
}

void
Dbtup::disk_page_prealloc_dirty_page(Disk_alloc_info & alloc,
				     PagePtr pagePtr, 
				     Uint32 old_idx,
                                     Uint32 sz,
                                     Fragrecord *fragPtrP)
{
  jam();
  jamLine(pagePtr.i);
  ddrequire(pagePtr.p->list_index == old_idx);

  Uint32 free= pagePtr.p->free_space;
  Uint32 used= pagePtr.p->uncommitted_used_space + sz;
  Uint32 ext= pagePtr.p->m_extent_info_ptr;
  
  ddrequire(free >= used);
  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, ext));

  Uint32 new_idx= alloc.calc_page_free_bits(free - used);
  pagePtr.p->uncommitted_used_space = used;

  if (old_idx != new_idx)
  {
    jam();
    disk_page_move_dirty_page(alloc,
                              extentPtr,
                              pagePtr,
                              old_idx,
                              new_idx,
                              fragPtrP);
  }

  DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)"
                  ", free: %u, used: %u",
                  instance(),
                  __LINE__,
                  pagePtr.p->m_file_no,
                  pagePtr.p->m_page_no,
                  free,
                  used));
  update_extent_pos(jamBuffer(),
                    fragPtrP,
                    extentPtr,
                    old_idx,
                    new_idx,
                    -Int32(sz));
}


void
Dbtup::disk_page_prealloc_transit_page(Fragrecord *fragPtrP,
				       Ptr<Page_request> req, 
				       Uint32 old_idx,
                                       Uint32 sz)
{
  jam();
  Disk_alloc_info& alloc = fragPtrP->m_disk_alloc_info;
  ddrequire(req.p->m_list_index == old_idx);

  Uint32 free= req.p->m_original_estimated_free_space;
  Uint32 used= req.p->m_uncommitted_used_space + sz;
  Uint32 ext= req.p->m_extent_info_ptr;
  
  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, ext));

  ddrequire(free >= used);
  Uint32 new_idx= alloc.calc_page_free_bits(free - used);
  
  if (old_idx != new_idx)
  {
    jam();
    disk_page_move_page_request(alloc, extentPtr, req, old_idx, new_idx);
  }

  req.p->m_uncommitted_used_space = used;
  DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)"
                  ", free: %u, used: %u",
                  instance(),
                  __LINE__,
                  req.p->m_key.m_file_no,
                  req.p->m_key.m_page_no,
                  free,
                  used));
  update_extent_pos(jamBuffer(),
                    fragPtrP,
                    extentPtr,
                    old_idx,
                    new_idx,
                    -Int32(sz));
}

void
Dbtup::disk_page_prealloc_callback(Signal* signal, 
				   Uint32 page_request, Uint32 page_id)
{
  jamEntry();

  Ptr<Page_request> req;
  ndbrequire(c_page_request_pool.getPtr(req, page_request));

  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));

  FragrecordPtr fragPtr;
  fragPtr.i= req.p->m_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));

  PagePtr pagePtr;
  pagePtr.i = gpage.i;
  pagePtr.p = reinterpret_cast<Page*>(gpage.p);

  Disk_alloc_info& alloc= fragPtr.p->m_disk_alloc_info;

  Local_key key = req.p->m_key;
  if (key.m_file_no != pagePtr.p->m_file_no ||
      key.m_page_no != pagePtr.p->m_page_no ||
      fragPtr.p->fragTableId != pagePtr.p->m_table_id ||
      fragPtr.p->fragmentId != pagePtr.p->m_fragment_id ||
      pagePtr.p->m_restart_seq == 0)
  {
    jam();
    /**
     * At this point we are reading what should be an initialised page
     * and thus file_no, page_no, table and fragment id should be correct.
     * If not crash and provide details.
     */
    g_eventLogger->info("(%u)key(%u,%u), page(%u,%u), restart_seq(%u,%u)"
                        "key_tab(%u,%u), page_tab(%u,%u)",
                        instance(),
                        key.m_file_no,
                        key.m_page_no,
                        pagePtr.p->m_file_no,
                        pagePtr.p->m_page_no,
                        globalData.m_restart_seq,
                        pagePtr.p->m_restart_seq,
                        fragPtr.p->fragTableId,
                        fragPtr.p->fragmentId,
                        pagePtr.p->m_table_id,
                        pagePtr.p->m_fragment_id);
    ndbabort();
  }
  if (unlikely(pagePtr.p->m_restart_seq != globalData.m_restart_seq))
  {
    jam();
    D(V(pagePtr.p->m_restart_seq) << V(globalData.m_restart_seq));
    restart_setup_page(fragPtr.p,
                       alloc,
                       pagePtr,
                       req.p->m_original_estimated_free_space);
  }

  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, req.p->m_extent_info_ptr));

  pagePtr.p->uncommitted_used_space += req.p->m_uncommitted_used_space;
  ddrequire(pagePtr.p->free_space >= pagePtr.p->uncommitted_used_space);

  Uint32 free = pagePtr.p->free_space - pagePtr.p->uncommitted_used_space;
  Uint32 idx = req.p->m_list_index;
  Uint32 real_idx = alloc.calc_page_free_bits(free);

  if (idx != real_idx)
  {
    jam();

    DEB_EXTENT_BITS((
      "(%u)extent(%u) page(%u,%u):%u u_u_s: %u, idx:%u, new_idx:%u"
      ", free_page_count(%u,%u), free: %u, used: %u",
      instance(),
      extentPtr.p->m_extent_no,
      pagePtr.p->m_file_no,
      pagePtr.p->m_page_no,
      pagePtr.i,
      free,
      idx,
      real_idx,
      extentPtr.p->m_free_page_count[idx],
      extentPtr.p->m_free_page_count[real_idx],
      pagePtr.p->uncommitted_used_space,
      free));

    ddrequire(extentPtr.p->m_free_page_count[idx] > 0);
    extentPtr.p->m_free_page_count[idx]--;
    extentPtr.p->m_free_page_count[real_idx]++;
    DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)",
                    instance(),
                    __LINE__,
                    pagePtr.p->m_file_no,
                    pagePtr.p->m_page_no));
    update_extent_pos(jamBuffer(),
                      fragPtr.p, extentPtr,
                      idx,
                      real_idx,
                      0);
  }
  {
    /**
     * add to dirty list
     */
    pagePtr.p->list_index = real_idx;
    Page_pool *cheat_pool= (Page_pool*)&m_global_page_pool;
    Local_Page_list list(* cheat_pool, alloc.m_dirty_pages[real_idx]);
    list.addFirst(pagePtr);
  }

  {
    /**
     * release page request
     */
    Local_page_request_list list(c_page_request_pool,
				 alloc.m_page_requests[idx]);
    list.release(req);
  }
}

void
Dbtup::disk_page_move_dirty_page(Disk_alloc_info& alloc,
                                 Ptr<Extent_info> extentPtr,
                                 Ptr<Page> pagePtr,
                                 Uint32 old_idx,
                                 Uint32 new_idx,
                                 Fragrecord *fragPtrP)
{
  DEB_EXTENT_BITS(("(%u)dpmdp:extent(%u) page(%u,%u):%u, old_idx: %u,"
                   " new_idx: %u, free_page_count(%u,%u)"
                   ", free_space: %u, uncommitted_space: %u",
                   instance(),
                   extentPtr.p->m_extent_no,
                   pagePtr.p->m_file_no,
                   pagePtr.p->m_page_no,
                   pagePtr.i,
                   old_idx,
                   new_idx,
                   extentPtr.p->m_free_page_count[old_idx],
                   extentPtr.p->m_free_page_count[new_idx],
                   pagePtr.p->free_space,
                   pagePtr.p->uncommitted_used_space));

  ddrequire(extentPtr.p->m_free_page_count[old_idx] > 0);
  extentPtr.p->m_free_page_count[old_idx]--;
  extentPtr.p->m_free_page_count[new_idx]++;

  jam();
  Page_pool *pool= (Page_pool*)&m_global_page_pool;
  Local_Page_list new_list(*pool, alloc.m_dirty_pages[new_idx]);
  Local_Page_list old_list(*pool, alloc.m_dirty_pages[old_idx]);
  old_list.remove(pagePtr);
  new_list.addFirst(pagePtr);

  pagePtr.p->list_index = new_idx;
}

void
Dbtup::disk_page_move_page_request(Disk_alloc_info& alloc,
                                   Ptr<Extent_info> extentPtr,
                                   Ptr<Page_request> req,
                                   Uint32 old_idx, Uint32 new_idx)
{
  jam();
  Page_request_list::Head *lists = alloc.m_page_requests;
  Local_page_request_list old_list(c_page_request_pool, lists[old_idx]);
  Local_page_request_list new_list(c_page_request_pool, lists[new_idx]);
  old_list.remove(req);
  new_list.addLast(req);

  DEB_EXTENT_BITS(("(%u)dpmpqr:extent(%u) page(%u,%u), old_idx: %u new_idx: %u"
                   ", free_page_count(%u,%u)",
                   instance(),
                   extentPtr.p->m_extent_no,
                   req.p->m_key.m_file_no,
                   req.p->m_key.m_page_no,
                   old_idx,
                   new_idx,
                   extentPtr.p->m_free_page_count[old_idx],
                   extentPtr.p->m_free_page_count[new_idx]));

  ddrequire(extentPtr.p->m_free_page_count[old_idx] > 0);
  extentPtr.p->m_free_page_count[old_idx]--;
  extentPtr.p->m_free_page_count[new_idx]++;
  req.p->m_list_index= new_idx;

}

/**
 * We have read in a page which is at the moment empty. It is possible that
 * the information on this page is garbage since this could be our first
 * access to this page. It could even have belonged to another table that
 * was deleted before getting here. So we need to initialise the page header
 * at this point in time.
 */
void
Dbtup::disk_page_prealloc_initial_callback(Signal*signal, 
					   Uint32 page_request, 
					   Uint32 page_id)
{
  jamEntry();
  /**
   * 1) lookup page request
   * 2) lookup page
   * 3) lookup table
   * 4) init page (according to page type)
   * 5) call ordinary callback
   */
  Ptr<Page_request> req;
  ndbrequire(c_page_request_pool.getPtr(req, page_request));
  jamDataDebug(req.p->m_key.m_file_no);
  jamDataDebug(req.p->m_key.m_page_no);

  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  PagePtr pagePtr;
  pagePtr.i = gpage.i;
  pagePtr.p = reinterpret_cast<Page*>(gpage.p);

  FragrecordPtr fragPtr;
  fragPtr.i= req.p->m_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));

  Ptr<Tablerec> tabPtr;
  tabPtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tabPtr, cnoOfTablerec, tablerec);

  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, req.p->m_extent_info_ptr));

  /**
   * We can come here even when the page have been already initialised.
   *
   * Unfortunately there is no sure way of discovering if we are reusing
   * an already used disk page. The extent information isn't synchronised
   * together with the disk page itself. So it is perfectly possible to
   * allocate an extent and write a page in it and then restart and as
   * part of recovery processing the extent isn't any more a part of this
   * fragment. A new extent can be used and this can be any extent. So this
   * means that we can even allocate the same extent once more by the same
   * fragment after the restart.
   *
   * So we simply go ahead and write this new page as an initial page.
   * There are plenty of other safeguards against wrong use of disk
   * pages and checkpointing algorithms.
   */

  /**
   * Ensure that all unset header variables are set to 0.
   */
  memset((char*)pagePtr.p, 0, Page::HEADER_WORDS * 4);

  convertThPage((Fix_page*)pagePtr.p, tabPtr.p, DD);

  /**
   * We have acquired an empty page without reading it from
   * disk. The page might however have been used in the past
   * and thus UNDO log entries might have to be written at
   * recovery towards this page. To ensure those UNDO log entries
   * are executed we need to set the LSN of the page to the
   * current LSN number.
   *
   * The problem happens if we write the page before we have updated
   * the LSN of the page. In this case the page will be written with
   * LSN 0 which isn't ok if the page was previously used.
   */
  Uint32 logfile_group_id= fragPtr.p->m_logfile_group_id;
  Uint64 lsn;
  {
    Logfile_client lgman(this, c_lgman, logfile_group_id);
    lsn = lgman.get_latest_lsn();
  }
  Page_cache_client pgman(this, c_pgman);
  pgman.set_lsn(req.p->m_key, lsn);
  DEB_PGMAN_IO(("(%u) Get empty page (%u,%u) set LSN: %llu"
                ", used: %u, free: %u, idx: %u",
                instance(),
                req.p->m_key.m_file_no,
                req.p->m_key.m_page_no,
                lsn,
                req.p->m_uncommitted_used_space,
                pagePtr.p->free_space,
                req.p->m_list_index));

  jamDebug();
  jamDataDebug(req.p->m_uncommitted_used_space);

  pagePtr.p->m_page_no= req.p->m_key.m_page_no;
  pagePtr.p->m_file_no= req.p->m_key.m_file_no;
  pagePtr.p->m_table_id= fragPtr.p->fragTableId;
  pagePtr.p->m_ndb_version = htonl(NDB_DISK_V2);
  pagePtr.p->m_create_table_version =
    c_lqh->getCreateSchemaVersion(fragPtr.p->fragTableId);
  pagePtr.p->m_fragment_id = fragPtr.p->fragmentId;
  pagePtr.p->m_extent_no = extentPtr.p->m_extent_no; // logical extent no
  pagePtr.p->m_extent_info_ptr= req.p->m_extent_info_ptr;
  pagePtr.p->m_restart_seq = globalData.m_restart_seq;
  pagePtr.p->nextList = pagePtr.p->prevList = RNIL;
  pagePtr.p->list_index = req.p->m_list_index;
  pagePtr.p->uncommitted_used_space = req.p->m_uncommitted_used_space;

  Disk_alloc_info& alloc= fragPtr.p->m_disk_alloc_info;
  Uint32 idx = req.p->m_list_index;

#if defined(VM_TRACE) || defined(ERROR_INSERT)
  {
    Uint32 free = pagePtr.p->free_space - pagePtr.p->uncommitted_used_space;
    ddrequire(idx == alloc.calc_page_free_bits(free));
    ddrequire(pagePtr.p->free_space == req.p->m_original_estimated_free_space);
  }
#endif

  {
    /**
     * add to dirty list
     */
    Page_pool *cheat_pool= (Page_pool*)&m_global_page_pool;
    Local_Page_list list(* cheat_pool, alloc.m_dirty_pages[idx]);
    list.addFirst(pagePtr);
  }

  {
    /**
     * release page request
     */
    Local_page_request_list list(c_page_request_pool, 
				 alloc.m_page_requests[idx]);
    list.release(req);
  }
}

void
Dbtup::disk_page_set_dirty(PagePtr pagePtr, Fragrecord *fragPtrP)
{
  jam();
  Uint32 idx = pagePtr.p->list_index;
  if ((pagePtr.p->m_restart_seq == globalData.m_restart_seq) &&
      ((idx & 0x8000) == 0))
  {
    jam();
    /**
     * Already in dirty list
     */
    return ;
  }
  
  Local_key key;
  key.m_page_no = pagePtr.p->m_page_no;
  key.m_file_no = pagePtr.p->m_file_no;

  pagePtr.p->nextList = pagePtr.p->prevList = RNIL;

  if (DBG_DISK)
    ndbout << " disk_page_set_dirty " << key << endl;

  ndbrequire(fragPtrP->fragTableId == pagePtr.p->m_table_id);
  ndbrequire(fragPtrP->fragmentId == pagePtr.p->m_fragment_id);
  
  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;

  Uint32 free = pagePtr.p->free_space;
  Uint32 used = pagePtr.p->uncommitted_used_space;
  if (unlikely(pagePtr.p->m_restart_seq != globalData.m_restart_seq))
  {
    jam();
#ifdef DEBUG_EXTENT_BITS
    Uint32 restart_seq = pagePtr.p->m_restart_seq;
#endif
    D(V(pagePtr.p->m_restart_seq) << V(globalData.m_restart_seq));
    restart_setup_page(fragPtrP, alloc, pagePtr, -1);
    ndbrequire(free == pagePtr.p->free_space);
    free = pagePtr.p->free_space;
    idx = alloc.calc_page_free_bits(free);
    used = 0;
    DEB_EXTENT_BITS(("((%u)restart_setup_page on page(%u,%u):%u"
                     ", idx = %u, free: %u, used: %u, restart_seq(%u,%u)",
                     instance(),
                     pagePtr.p->m_file_no,
                     pagePtr.p->m_page_no,
                     pagePtr.i,
                     idx,
                     free,
                     used,
                     restart_seq,
                     globalData.m_restart_seq));
  }
  else
  {
    jam();
    idx &= ~0x8000;
    DEB_EXTENT_BITS(("((%u)Reset list_index bit 0x8000 on page(%u,%u):%u"
                     ", idx = %u, free: %u, used: %u",
                     instance(),
                     pagePtr.p->m_file_no,
                     pagePtr.p->m_page_no,
                     pagePtr.i,
                     idx,
                     free,
                     used));
    ddrequire(idx == alloc.calc_page_free_bits(free - used));
  }
  
  ddrequire(free >= used);
  
  D("Tablespace_client - disk_page_set_dirty");
  Tablespace_client tsman(0, this, c_tsman,
                        fragPtrP->fragTableId,
                        fragPtrP->fragmentId,
                        c_lqh->getCreateSchemaVersion(fragPtrP->fragTableId),
                        fragPtrP->m_tablespace_id);
  
  pagePtr.p->list_index = idx;
  Page_pool *pool= (Page_pool*)&m_global_page_pool;
  Local_Page_list list(*pool, alloc.m_dirty_pages[idx]);
  list.addFirst(pagePtr);
  
  // Make sure no one will allocate it...
  tsman.unmap_page(&key, EXTENT_SEARCH_MATRIX_COLS - 1);
  jamEntry();
}

void
Dbtup::disk_page_dirty_header(Signal *signal,
                              Fragrecord* regFragPtr,
                              Local_key key,
                              PagePtr pagePtr,
                              Int32 add)
{
  /**
   * We need to set uncommitted_used_space on the page and if so required
   * also the m_restart_seq variable.
   *
   * It is possible that the page coming here isn't yet a dirty page.
   * Since this is executed in the prepare phase we need not make it a
   * normal dirty page. This would mean that the page is required to be
   * checkpointed. However we only need to make sure that the page isn't
   * released from page cache without being written to disk.
   *
   * We accomplish this by using the new interface DIRTY_HEADER that
   * makes the page dirty without putting it into fragment dirty list.
   * Thus avoiding being unnecessarily checkpointed.
   *
   * Since we are ensuring that the page will be written to disk before
   * being released, it is a good idea to also put it into the dirty list.
   * This ensures that the page is used for future allocations of new
   * rows for as long as it stays in the page cache or get written to disk.
   * To put it into the dirty list isn't required, but still makes sense.
   *
   * The actual update of the uncommitted_used_space happens in
   * disk_page_prealloc_dirty_page, this method also ensures that the
   * extent information about pages is kept up to date and that the
   * list_index variable is properly set according to the remaining
   * free space (when preallocated space is subtracted).
   */
  if (add < 0)
  {
    Uint32 overflow_space = Uint32(-add);
    disk_page_abort_prealloc_callback_1(signal,
                                        regFragPtr,
                                        pagePtr,
                                        overflow_space,
                                        0);
  }
  else
  {
    disk_page_set_dirty(pagePtr, regFragPtr);
    disk_page_prealloc_dirty_page(regFragPtr->m_disk_alloc_info,
                                  pagePtr,
                                  pagePtr.p->list_index,
                                  Uint32(add),
                                  regFragPtr);
  }

  Page_cache_client::Request preq;
  preq.m_page = key;
  preq.m_table_id = regFragPtr->fragTableId;
  preq.m_fragment_id = regFragPtr->fragmentId;
  preq.m_callback.m_callbackData= RNIL;
  preq.m_callback.m_callbackFunction= 
    safe_cast(&Dbtup::disk_page_dirty_header_callback);
  Uint32 flags = Page_cache_client::DIRTY_HEADER;

  Page_cache_client pgman(this, c_pgman);
  int res= pgman.get_page(signal, preq, flags);
  jamEntry();
  ndbrequire(res == 1);
}

void
Dbtup::disk_page_dirty_header_callback(Signal *signal,
                                       Uint32 opRec,
                                       Uint32 page_id)
{
  (void)signal;
  (void)opRec;
  (void)page_id;
  ndbabort();
}

void
Dbtup::disk_page_unmap_callback(Uint32 when,
				Uint32 page_id,
                                Uint32 dirty_count,
                                Uint32 ptrI)
{
  jamEntry();
  (void)ptrI;
  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  PagePtr pagePtr;
  pagePtr.i = gpage.i;
  pagePtr.p = reinterpret_cast<Page*>(gpage.p);
  
  Uint32 type = pagePtr.p->m_page_header.m_page_type;
  if (unlikely((type != File_formats::PT_Tup_fixsize_page &&
		type != File_formats::PT_Tup_varsize_page) ||
	       f_undo_done == false))
  {
    jam();
    D("disk_page_unmap_callback" << V(type) << V(f_undo_done));
    return ;
  }

  Uint32 idx = pagePtr.p->list_index;

  Uint32 tableId = pagePtr.p->m_table_id;
  
  FragrecordPtr fragPtr;
  getFragmentrec(fragPtr, pagePtr.p->m_fragment_id, tableId);
  ndbrequire(fragPtr.i != RNIL64);

  DEB_PGMAN_IO(("(%u)unmap page: tab(%u,%u), page(%u,%u):%u,"
                " lsn(%u,%u),when:%u,dirty:%u, ptr.i : %u",
                instance(),
                pagePtr.p->m_table_id,
                pagePtr.p->m_fragment_id,
                pagePtr.p->m_file_no,
                pagePtr.p->m_page_no,
                pagePtr.i,
                pagePtr.p->m_page_header.m_page_lsn_hi,
                pagePtr.p->m_page_header.m_page_lsn_lo,
                when,
                dirty_count,
                ptrI));

  Disk_alloc_info& alloc= fragPtr.p->m_disk_alloc_info;
  
  if (when == 0)
  {
    /**
     * Before pageout
     */
    jam();

    if (DBG_DISK)
    {
      Local_key key;
      key.m_page_no = pagePtr.p->m_page_no;
      key.m_file_no = pagePtr.p->m_file_no;
      ndbout << "disk_page_unmap_callback(before) " << key 
	     << " cnt: " << dirty_count << " " << (idx & ~0x8000) << endl;
    }

    /**
     * Pages being written to disk are always in the dirty page list in
     * DBTUP. This is true also for pages using the DIRTY_HEADER interface.
     */
    ndbrequire((idx & 0x8000) == 0);

    /**
     * Page is being written to disk, remove from dirty page list, we cannot
     * use this page while it is being paged out.
     *
     * Insert into m_unmap_pages, this ensures that we handle those pages
     * correctly during Drop Table execution.
     */
    Page_pool *pool= (Page_pool*)&m_global_page_pool;
    Local_Page_list list(*pool, alloc.m_dirty_pages[idx]);
    Local_Page_list list2(*pool, alloc.m_unmap_pages);
    list.remove(pagePtr);
    list2.addFirst(pagePtr);

    /**
     * Ensure that we always set the bit indicating that we're not in any
     * of the dirty lists. When a page arrives from the disk, it should never
     * be in any of the dirty page lists in DBTUP.
     */
    pagePtr.p->list_index = idx | 0x8000;

    if (dirty_count == 0)
    {
      jam();
      Local_key key;
      key.m_page_no = pagePtr.p->m_page_no;
      key.m_file_no = pagePtr.p->m_file_no;
      
      Uint32 free = pagePtr.p->free_space;
      Uint32 used = pagePtr.p->uncommitted_used_space;

      DEB_EXTENT_BITS(("(%u)Set list_index bit 0x8000 on page(%u,%u)"
                       " when unmap, used: %u, free: %u, idx: %u",
                       instance(),
                       pagePtr.p->m_file_no,
                       pagePtr.p->m_page_no,
                       used,
                       free,
                       idx));
      
      ddrequire(free >= used);
      ddrequire(alloc.calc_page_free_bits(free - used) == idx);
      
      D("Tablespace_client - disk_page_unmap_callback");
      Tablespace_client tsman(0, this, c_tsman,
                    fragPtr.p->fragTableId,
                    fragPtr.p->fragmentId,
                    c_lqh->getCreateSchemaVersion(fragPtr.p->fragTableId),
                    fragPtr.p->m_tablespace_id);
      
      tsman.unmap_page(&key, idx);
      jamEntry();
    }
  }
  else if (when == 1)
  {
    /**
     * After page out
     */
    jam();

    Local_key key;
    key.m_page_no = pagePtr.p->m_page_no;
    key.m_file_no = pagePtr.p->m_file_no;
    Uint32 real_free = pagePtr.p->free_space;
    
    if (DBG_DISK)
    {
      ndbout << "disk_page_unmap_callback(after) " << key 
	     << " cnt: " << dirty_count << " " << (idx & ~0x8000) << endl;
    }

    Page_pool *pool= (Page_pool*)&m_global_page_pool;
    Local_Page_list list(*pool, alloc.m_unmap_pages);
    list.remove(pagePtr);

    D("Tablespace_client - disk_page_unmap_callback");
    Tablespace_client tsman(0, this, c_tsman,
                   fragPtr.p->fragTableId,
                   fragPtr.p->fragmentId,
                   c_lqh->getCreateSchemaVersion(fragPtr.p->fragTableId),
                   fragPtr.p->m_tablespace_id);
    
    if (DBG_DISK && alloc.calc_page_free_bits(real_free) != (idx & ~0x8000))
    {
      ndbout << key 
	     << " calc: " << alloc.calc_page_free_bits(real_free)
	     << " idx: " << (idx & ~0x8000)
	     << endl;
    }
    DEB_EXTENT_BITS(("(%u)tab(%u,%u), page(%u,%u):%u real_free: %u,"
                     " new_bits: %u",
                    instance(),
                    fragPtr.p->fragTableId,
                    fragPtr.p->fragmentId,
                    pagePtr.p->m_file_no,
                    pagePtr.p->m_page_no,
                    pagePtr.i,
                    real_free,
                    alloc.calc_page_free_bits(real_free)));

    tsman.update_page_free_bits(&key, alloc.calc_page_free_bits(real_free));
    jamEntry();
  }
}

void
Dbtup::disk_page_alloc(Signal* signal, 
		       Tablerec* tabPtrP,
                       Fragrecord* fragPtrP, 
		       Local_key* key,
                       PagePtr pagePtr,
                       Uint32 gci,
                       const Local_key *row_id,
                       Uint32 undo_size,
                       Uint32 extra_alloc)
{
  jam();
  Uint32 logfile_group_id= fragPtrP->m_logfile_group_id;
  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;

  Uint64 lsn;
  if ((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    jam();
    ddrequire(pagePtr.p->uncommitted_used_space > 0);
    pagePtr.p->uncommitted_used_space--;
    key->m_page_idx= ((Fix_page*)pagePtr.p)->alloc_record();
    jamLine(Uint16(key->m_page_idx));
    lsn= disk_page_undo_alloc(signal,
                              pagePtr.p,
                              key,
                              gci,
                              logfile_group_id,
                              undo_size);
  }
  else
  {
    jam();
    Uint32 sz = key->m_page_idx;
    Uint32 old_free = pagePtr.p->free_space;
    Uint32 old_used = pagePtr.p->uncommitted_used_space;
    ddrequire(old_free >= old_used);
    ddrequire(pagePtr.p->uncommitted_used_space >= (sz + extra_alloc));
    pagePtr.p->uncommitted_used_space -= (sz + extra_alloc);
    key->m_page_idx= ((Var_page*)pagePtr.p)->
      alloc_record(sz, (Var_page*)ctemp_page, 0);

    /**
     * We have allocated space on the page, this might include increasing
     * the directory space or not. Normally we take height for increasing
     * the directory, thus we could have used less space than we
     * requested as uncommitted used space and this might move the page
     * to a new extent index although improbable.
     *
     * TODO RONM: Need to investigate how this should affect the method
     * update_extent_pos.
     */
    Uint32 old_idx = pagePtr.p->list_index;
    Uint32 new_free = pagePtr.p->free_space;
    Uint32 used = pagePtr.p->uncommitted_used_space;
    ndbrequire(new_free >= used);
    Uint32 new_idx = alloc.calc_page_free_bits(new_free - used);
    if (new_idx != old_idx)
    {
      jam();
      Uint32 ext = pagePtr.p->m_extent_info_ptr;
      Ptr<Extent_info> extentPtr;
      ndbrequire(c_extent_pool.getPtr(extentPtr, ext));
      disk_page_move_dirty_page(alloc,
                                extentPtr,
                                pagePtr,
                                old_idx,
                                new_idx,
                                fragPtrP);
      Int32 sz = (new_free - used) - (old_free - old_used);
      update_extent_pos(jamBuffer(),
                        fragPtrP,
                        extentPtr,
                        old_idx,
                        new_idx,
                        sz);
    }
    lsn= disk_page_undo_alloc(signal,
                              pagePtr.p,
                              key,
                              gci,
                              logfile_group_id,
                              undo_size);
  }
  DEB_PGMAN((
    "(%u)disk_page_alloc: tab(%u,%u):%u,page(%u,%u).%u.%u,gci: %u,"
    "row_id(%u,%u), lsn=%llu, alloc_size: %u, free: %u, used: %u",
              instance(),
              pagePtr.p->m_table_id,
              pagePtr.p->m_fragment_id,
              pagePtr.p->m_create_table_version,
              key->m_file_no,
              key->m_page_no,
              key->m_page_idx,
              pagePtr.i,
              gci,
              row_id->m_page_no,
              row_id->m_page_idx,
              lsn,
              undo_size,
              pagePtr.p->free_space,
              pagePtr.p->uncommitted_used_space));
}

void
Dbtup::disk_page_free(Signal *signal, 
		      Tablerec *tabPtrP,
                      Fragrecord * fragPtrP,
		      const Local_key* key,
                      PagePtr pagePtr,
                      Uint32 gci,
                      const Local_key *row_id,
                      Uint32 undo_len)
{
  jam();
  if (DBG_DISK)
    ndbout << " disk_page_free " << *key << endl;
  
  Uint32 page_idx= key->m_page_idx;
  jamLine(Uint16(key->m_page_idx));
  Uint32 logfile_group_id= fragPtrP->m_logfile_group_id;
  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;
  Uint32 old_free= pagePtr.p->free_space;

  Uint32 sz;
  Uint64 lsn;
  if ((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    sz = 1;
    const Uint32 *src= ((Fix_page*)pagePtr.p)->get_ptr(page_idx, 0);
    if (!((*(src + 1)) < Tup_page::DATA_WORDS))
    {
      g_eventLogger->info(
        "(%u)disk_page_free crash:tab(%u,%u):%u,page(%u,%u).%u.%u"
        ",gci:%u,row(%u,%u), row_ref(%u,%u)",
                 instance(),
                 fragPtrP->fragTableId,
                 fragPtrP->fragmentId,
                 pagePtr.p->m_create_table_version,
                 pagePtr.p->m_file_no,
                 pagePtr.p->m_page_no,
                 page_idx,
                 pagePtr.i,
                 gci,
                 row_id->m_page_no,
                 row_id->m_page_idx,
                 *src,
                 *(src + 1));
      ndbrequire(((*(src + 1)) < Tup_page::DATA_WORDS));
    }
    lsn= disk_page_undo_free(signal,
                             pagePtr.p,
                             key,
			     src,
                             tabPtrP->m_offsets[DD].m_fix_header_size,
			     gci,
                             logfile_group_id,
                             undo_len,
                             false);
    ((Fix_page*)pagePtr.p)->free_record(page_idx);
  }
  else
  {
    jam();
    const Uint32 *src = ((Var_page*)pagePtr.p)->get_ptr(page_idx);
    sz = ((Var_page*)pagePtr.p)->get_entry_len(page_idx);
    Uint32 size_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2);
    Uint32 new_undo_len = size_len + (sz - 1);
    jamDataDebug(new_undo_len);
    jamDataDebug(undo_len);
    /**
     * We allocate 1 extra word per kByte, thus we should have at
     * least new_undo_len words, but at most new_undo_len + 32
     * words preallocated.
     */
    ndbrequire(undo_len >= new_undo_len);
    ndbrequire((new_undo_len + 32) >= undo_len);
    lsn = disk_page_undo_free(signal,
                              pagePtr.p,
                              key,
			      src,
                              sz,
			      gci,
                              logfile_group_id,
                              undo_len,
                              true);
    
    ((Var_page*)pagePtr.p)->free_record(page_idx, 0);
  }
  DEB_PGMAN((
    "(%u)disk_page_free:tab(%u,%u):%u,page(%u,%u).%u.%u,gci:%u,row(%u,%u)"
    ", lsn=%llu, new_undo_len: %u, undo_len: %u",
             instance(),
             fragPtrP->fragTableId,
             fragPtrP->fragmentId,
             pagePtr.p->m_create_table_version,
             pagePtr.p->m_file_no,
             pagePtr.p->m_page_no,
             page_idx,
             pagePtr.i,
             gci,
             row_id->m_page_no,
             row_id->m_page_idx,
             lsn,
             new_undo_len,
             undo_len));


  /**
   * We have free'd space on the page, this might include free'ing some
   * directory space, thus it is important to here check that the page
   * is still in the correct extent index (0-3). If not move it to the
   * new extent index.
   */
  Uint32 new_free = pagePtr.p->free_space;
  Uint32 ext = pagePtr.p->m_extent_info_ptr;
  Uint32 used = pagePtr.p->uncommitted_used_space;
  Uint32 old_idx = pagePtr.p->list_index;
  ddrequire(old_free >= used);
  ddrequire(new_free >= used);
  ddrequire(new_free >= old_free);
  ddrequire((old_idx & 0x8000) == 0);

  Uint32 new_idx = alloc.calc_page_free_bits(new_free - used);
  ddrequire(alloc.calc_page_free_bits(old_free - used) == old_idx);
  
  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, ext));
  
  if (old_idx != new_idx)
  {
    jam();
    disk_page_move_dirty_page(alloc,
                              extentPtr,
                              pagePtr,
                              old_idx,
                              new_idx,
                              fragPtrP);
  }
  DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)"
                  ", used: %u, old_free: %u, new_free: %u",
                  instance(),
                  __LINE__,
                  pagePtr.p->m_file_no,
                  pagePtr.p->m_page_no,
                  used,
                  old_free,
                  new_free));
  Int32 change = new_free - used;
  update_extent_pos(jamBuffer(),
                    fragPtrP,
                    extentPtr,
                    old_idx,
                    new_idx,
                    change);
}

void
Dbtup::disk_page_abort_prealloc(Signal *signal, Fragrecord* fragPtrP, 
				Local_key* key, Uint32 sz)
{
  jam();

  Page_cache_client::Request req;
  req.m_callback.m_callbackData= sz;
  req.m_callback.m_callbackFunction = 
    safe_cast(&Dbtup::disk_page_abort_prealloc_callback);
  
  int flags= Page_cache_client::ABORT_REQ;
  memcpy(&req.m_page, key, sizeof(Local_key));
  req.m_table_id = fragPtrP->fragTableId;
  req.m_fragment_id = fragPtrP->fragmentId;

  Page_cache_client pgman(this, c_pgman);
  int res= pgman.get_page(signal, req, flags);
  jamEntry();
  switch(res)
  {
  case 0:
    jam();
    c_lqh->increment_usage_count_for_table(req.m_table_id);
    break;
  case -1:
    ndbabort();
  default:
    jam();
    ndbrequire(res > 0);
    Ptr<GlobalPage> gpage;
    ndbrequire(m_global_page_pool.getPtr(gpage, (Uint32)res));
    PagePtr pagePtr;
    pagePtr.i = gpage.i;
    pagePtr.p = reinterpret_cast<Page*>(gpage.p);

    disk_page_abort_prealloc_callback_1(signal, fragPtrP, pagePtr, sz, 0);
  }
}

void
Dbtup::disk_page_abort_prealloc_callback(Signal* signal, 
					 Uint32 sz, Uint32 page_id)
{
  jamEntry();  
  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  
  PagePtr pagePtr;
  pagePtr.i = gpage.i;
  pagePtr.p = reinterpret_cast<Page*>(gpage.p);

  Uint32 tableId = pagePtr.p->m_table_id;

  c_lqh->decrement_usage_count_for_table(tableId);

  FragrecordPtr fragPtr;
  getFragmentrec(fragPtr, pagePtr.p->m_fragment_id, tableId);
  ndbrequire(fragPtr.i != RNIL64);

  disk_page_abort_prealloc_callback_1(signal, fragPtr.p, pagePtr, sz, 0);
}

void
Dbtup::disk_page_abort_prealloc_callback_1(Signal* signal, 
					   Fragrecord* fragPtrP,
					   PagePtr pagePtr,
					   Uint32 sz,
                                           Int32 change)
{
  jam();
  disk_page_set_dirty(pagePtr, fragPtrP);

  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;

  Ptr<Extent_info> extentPtr;
  ndbrequire(c_extent_pool.getPtr(extentPtr, pagePtr.p->m_extent_info_ptr));

  Uint32 idx = pagePtr.p->list_index & 0x7FFF;
  Uint32 used = pagePtr.p->uncommitted_used_space;
  Uint32 free = pagePtr.p->free_space;

  ddrequire(free >= used);
  ddrequire(used >= sz);
  if (sz > 0)
  {
    ddrequire(alloc.calc_page_free_bits(free - used) == idx);
  }

  pagePtr.p->uncommitted_used_space = used - sz;

  Uint32 new_idx = alloc.calc_page_free_bits(free - (used - sz));

  if (idx != new_idx)
  {
    jam();
    disk_page_move_dirty_page(alloc,
                              extentPtr,
                              pagePtr,
                              idx,
                              new_idx,
                              fragPtrP);
  }
  
  DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)"
                  ", free: %u, used: %u, new_idx: %u",
                  instance(),
                  __LINE__,
                  pagePtr.p->m_file_no,
                  pagePtr.p->m_page_no,
                  free,
                  pagePtr.p->uncommitted_used_space,
                  new_idx));
  change += sz;
  update_extent_pos(jamBuffer(),
                    fragPtrP,
                    extentPtr,
                    idx,
                    new_idx,
                    change);
}

Uint64
Dbtup::disk_page_undo_alloc(Signal *signal,
                            Page* page,
                            const Local_key* key,
                            Uint32 gci,
                            Uint32 logfile_group_id,
                            Uint32 alloc_size)
{
  jam();
  Disk_undo::Alloc alloc;
  alloc.m_type_length= (Disk_undo::UNDO_ALLOC << 16) | (sizeof(alloc) >> 2);
  alloc.m_page_no = key->m_page_no;
  alloc.m_file_no_page_idx= key->m_file_no << 16 | key->m_page_idx;
  
  Logfile_client::Change c[1] = {{ &alloc, sizeof(alloc) >> 2 } };
  
  Uint64 lsn;
  {
    D("Logfile_client - disk_page_undo_alloc");
    Logfile_client lgman(this, c_lgman, logfile_group_id);
    lsn= lgman.add_entry_simple(c, 1, alloc_size);
  }
  jamEntry();
  {
    Page_cache_client pgman(this, c_pgman);
    pgman.update_lsn(signal, * key, lsn);
  }
  jamEntry();

  return lsn;
}

Uint64
Dbtup::disk_page_undo_update(Signal *signal,
                             Page* page,
                             const Local_key* key,
			     const Uint32* src,
                             Uint32 sz,
			     Uint32 gci,
                             Uint32 logfile_group_id,
                             Uint32 alloc_size,
                             bool var_disk)
{
  jam();

  Disk_undo::Update_Free update;
  update.m_page_no = key->m_page_no;
  update.m_file_no_page_idx= key->m_file_no << 16 | key->m_page_idx;
  update.m_gci= gci;
  
  update.m_type_length= 
    (Disk_undo::UNDO_UPDATE << 16) | (sz + (sizeof(update) >> 2) - 1);

  Logfile_client::Change c[3] = {
    { &update, 3 },
    { src, sz },
    { &update.m_type_length, 1 }
  };

  ndbassert(4*(3 + sz + 1) == (sizeof(update) + 4*sz - 4));

  Uint64 lsn;
  {
    D("Logfile_client - disk_page_undo_update");
    Logfile_client lgman(this, c_lgman, logfile_group_id);
    lsn= lgman.add_entry_complex(c, 3, true, alloc_size, var_disk);
  }
  jamEntry();
  {
    Page_cache_client pgman(this, c_pgman);
    pgman.update_lsn(signal, * key, lsn);
  }
  jamEntry();

  return lsn;
}
  
Uint64
Dbtup::disk_page_undo_free(Signal *signal,
                           Page* page,
                           const Local_key* key,
			   const Uint32* src,
                           Uint32 sz,
			   Uint32 gci,
                           Uint32 logfile_group_id,
                           Uint32 alloc_size,
                           bool var_disk)
{
  jam();

  Disk_undo::Update_Free free;
  free.m_page_no = key->m_page_no;
  free.m_file_no_page_idx= key->m_file_no << 16 | key->m_page_idx;
  free.m_gci= gci;
  
  free.m_type_length= 
    (Disk_undo::UNDO_FREE << 16) | (sz + (sizeof(free) >> 2) - 1);
  
  Logfile_client::Change c[3] = {
    { &free, 3 },
    { src, sz },
    { &free.m_type_length, 1 }
  };
  
  ndbassert(4*(3 + sz + 1) == (sizeof(free) + 4*sz - 4));
  
  Uint64 lsn;
  {
    D("Logfile_client - disk_page_undo_free");
    Logfile_client lgman(this, c_lgman, logfile_group_id);
    lsn= lgman.add_entry_complex(c, 3, false, alloc_size, var_disk);
  }
  jamEntry();
  {
    Page_cache_client pgman(this, c_pgman);
    pgman.update_lsn(signal, * key, lsn);
  }
  jamEntry();
  return lsn;
}
  
#define DBG_UNDO 0

void
Dbtup::verify_undo_log_execution()
{
  ndbrequire(!f_undo.m_in_intermediate_log_record);
}

/**
 * Preface:
 * With parallel undo log application, many undo records can be sent to the
 * LDM threads without waiting for the LDM threads to finish applying them.
 *
 * Before applying a log record, we must fetch the page (get_page) and
 * sometimes, if the page is not available immediately, we have to wait for it
 * before the log record can be applied. Waiting is done by periodically
 * checking if the page is available (do_busy_loop()).
 * However, between the checks, a subsequent log record belonging to the same
 * page might get processed. This is because multiple log records are sent from
 * LGMAN to the LDM threads continuously without waiting for the LDM threads to
 * finish applying them. (WL #8478)
 * This subsequent log record will try to get the page as well and might
 * succeed. This will result in unordered application of the undo records.
 *
 * The solution for this is to order the undo records belonging to a page.
 *
 * Algorithm for ordering record types which require disk page requests:
 * (UNDO_TUP_ALLOC, UNDO_TUP_UPDATE, UNDO_TUP_UPDATE_PART, UNDO_TUP_UPDATE_PART
 * , UNDO_TUP_FREE, UNDO_TUP_FREE_PART)
 *
 * c_undo_page_hash holds all the pages (of type Pending_undo_page) which
 * have requests pending. Each Pending_undo_page has a list of pending undo
 * records (of type Apply_undo) for that page.
 *
 * First, the page to which the current record being processed belongs is
 * searched in the hash table(c_undo_page_hash).
 * If it exists, the current undo record is added to the list of pending undo
 * records of the page.
 *
 * If the page isn't present in the hash table, it means there are no pending
 * requests for that page and the page is requested from PGMAN.
 * If the page is not available at the moment, it is added to the hash table
 * and the current undo record being processed is added to the pending list of
 * the page.
 * When the page is available immediately, the callback which applies the
 * undo records (disk_restart_undo_callback()) is executed.
 */
void
Dbtup::disk_restart_undo(Signal* signal,
                         Uint64 lsn,
			 Uint32 type,
                         const Uint32 * ptr,
                         Uint32 len)
{
  f_undo_done = false;
  f_undo.m_lsn= lsn;
  f_undo.m_ptr= ptr;
  f_undo.m_len= len;
  f_undo.m_type = type;

  Page_cache_client::Request preq;
  switch(f_undo.m_type){
  case File_formats::Undofile::UNDO_LOCAL_LCP_FIRST:
  case File_formats::Undofile::UNDO_LOCAL_LCP:
  case File_formats::Undofile::UNDO_LCP_FIRST:
  case File_formats::Undofile::UNDO_LCP:
  {
    /**
     * Searching for end of UNDO log execution is only done in
     * lgman.cpp. So here we assume that we are supposed to continue
     * executing the UNDO log. So no checks for end in this logic.
     */
    jam();
    Uint32 lcpId;
    Uint32 localLcpId;
    Uint32 tableId;
    Uint32 fragId;
    if (f_undo.m_type == File_formats::Undofile::UNDO_LOCAL_LCP ||
        f_undo.m_type == File_formats::Undofile::UNDO_LOCAL_LCP_FIRST)
    {
      jam();
      ndbrequire(len == 4);
      lcpId = ptr[0];
      localLcpId = ptr[1];
      tableId = ptr[2] >> 16;
      fragId = ptr[2] & 0xFFFF;
    }
    else
    {
      jam();
      ndbrequire(len == 3);
      lcpId = ptr[0];
      localLcpId = 0;
      tableId = ptr[1] >> 16;
      fragId = ptr[1] & 0xFFFF;
    }
    if (tableId != 0)
    {
      jam();
      disk_restart_undo_lcp(tableId,
                            fragId,
                            Fragrecord::UC_LCP,
                            lcpId,
                            localLcpId,
                            lsn);
    }
    if (!isNdbMtLqh())
      disk_restart_undo_next(signal);
    
    DEB_UNDO_LCP(("(%u)UNDO LCP [%u,%u] tab(%u,%u)",
                  instance(),
                  lcpId,
                  localLcpId,
                  tableId,
                  fragId));
    return;
  }
  case File_formats::Undofile::UNDO_TUP_ALLOC:
  {
    jam();
    Disk_undo::Alloc* rec= (Disk_undo::Alloc*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = 0;
    f_undo.m_tot_len = 0;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_UPDATE:
  {
    jam();
    Disk_undo::Update_Free* rec= (Disk_undo::Update_Free*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = 0;
    f_undo.m_tot_len = 0;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_UPDATE_PART:
  case File_formats::Undofile::UNDO_TUP_UPDATE_VAR_PART:
  {
    jam();
    Disk_undo::UpdatePart* rec= (Disk_undo::UpdatePart*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = rec->m_offset;
    f_undo.m_tot_len = 0;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_FIRST_UPDATE_VAR_PART:
  case File_formats::Undofile::UNDO_TUP_FREE_VAR_PART:
  {
    jam();
    Disk_undo::Update_Free_FirstVarPart* rec=
      (Disk_undo::Update_Free_FirstVarPart*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = 0;
    f_undo.m_tot_len = rec->m_tot_len;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_FREE:
  {
    jam();
    Disk_undo::Update_Free* rec= (Disk_undo::Update_Free*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = 0;
    f_undo.m_tot_len = 0;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_FREE_PART:
  case File_formats::Undofile::UNDO_TUP_FIRST_UPDATE_PART:
  {
    jam();
    Disk_undo::Update_Free* rec= (Disk_undo::Update_Free*)ptr;
    preq.m_page.m_page_no = rec->m_page_no;
    preq.m_page.m_file_no  = rec->m_file_no_page_idx >> 16;
    preq.m_page.m_page_idx = rec->m_file_no_page_idx & 0xFFFF;
    f_undo.m_offset = 0;
    f_undo.m_tot_len = 0;
    break;
  }
  case File_formats::Undofile::UNDO_TUP_DROP:
  {
    jam();
    Disk_undo::Drop* rec = (Disk_undo::Drop*)ptr;
    /**
     * We could come here in a number of situations:
     * 1) It could be a record that belongs to a table that we are not
     *    restoring, in this case we won't find the table in the search
     *    below.
     * 2) It could belong to a table we are restoring, but this is a
     *    drop of a previous incarnation of this table. Definitely no
     *    more log records should be executed for this table.
     * 
     * Coming here after we reached the end of the fragment LCP should not
     * happen, so we insert an ndbrequire to ensure this doesn't happen.
     */
    Uint32 tableId = rec->m_table;
    if (tableId < cnoOfTablerec)
    {
      jam();
      DEB_UNDO_LCP(("(%u)UNDO_TUP_DROP: lsn: %llu, tab: %u",
                    instance(),
                    lsn,
                    tableId));
      for(Uint32 i = 0; i < MAX_FRAG_PER_LQH; i++)
      {
        jam();
        Uint32 fragId = c_lqh->getNextTupFragid(tableId, i);
        if (fragId != RNIL)
        {
          jam();
          jamLine(Uint16(fragId));
          disk_restart_undo_lcp(tableId, fragId,
                                Fragrecord::UC_DROP, 0, 0, lsn);
        }
      }
    }
    return;
  }
  case File_formats::Undofile::UNDO_END:
    jam();
    f_undo_done = true;
    ndbrequire(c_pending_undo_page_hash.getCount() == 0);
    return;
  default:
    ndbabort();
  }

  f_undo.m_key = preq.m_page;
  preq.m_table_id = (~0); /* Special code for table id for UNDO_REQ */
  preq.m_fragment_id = 0;
  preq.m_callback.m_callbackFunction = 
    safe_cast(&Dbtup::disk_restart_undo_callback);

  Ptr<Pending_undo_page> cur_undo_record_page;
  cur_undo_record_page.i = RNIL;
  cur_undo_record_page.p = nullptr;

  if (isNdbMtLqh())
  {
    jam();
    Pending_undo_page key(preq.m_page.m_file_no, preq.m_page.m_page_no);

    if (c_pending_undo_page_hash.find(cur_undo_record_page, key))
    {
      jam();
      /**
       *  Page of the current undo record being processed already has a pending
       *  request.
       */
      Ptr<Apply_undo> cur_undo_record;
      ndbrequire(c_apply_undo_pool.seize(cur_undo_record));

      f_undo.m_magic = cur_undo_record.p->m_magic;
      *(cur_undo_record.p) = f_undo;

      LocalApply_undo_list undoList(c_apply_undo_pool,
                                    cur_undo_record_page.p->m_apply_undo_head);
      // add to Apply_undo list of the page it belongs to
      undoList.addLast(cur_undo_record);
      DEB_UNDO(("LDM(%u) WAIT page(%u,%u) count:%u lsn:%llu,"
                " data[%u,%u,%u], pending.p = %p",
                instance(),
                preq.m_page.m_file_no,
                preq.m_page.m_page_no,
                undoList.getCount(),
                f_undo.m_lsn,
                f_undo.m_data[3],
                f_undo.m_data[4],
                f_undo.m_data[5],
                cur_undo_record.p));
      ndbrequire(undoList.getCount() <= MAX_PENDING_UNDO_RECORDS);
      return;
    }

    // page doesn't have any pending request
    // allocate for cur_undo_record_page from pool
    ndbrequire(c_pending_undo_page_pool.seize(cur_undo_record_page));
    preq.m_callback.m_callbackData = cur_undo_record_page.i;
  }

  int flags = Page_cache_client::UNDO_REQ;
  Page_cache_client pgman(this, c_pgman);
  int res= pgman.get_page(signal, preq, flags);

  jamEntry();

  switch(res)
  {
  case 0:
    jam();
    m_immediate_flag = false;

    if (isNdbMtLqh())
    {
      //initialize page, add to hash table
      new(cur_undo_record_page.p)
          Pending_undo_page(preq.m_page.m_file_no, preq.m_page.m_page_no);
      c_pending_undo_page_hash.add(cur_undo_record_page);

      //add undo record to list
      Ptr<Apply_undo> cur_undo_record;
      ndbrequire(c_apply_undo_pool.seize(cur_undo_record));

      f_undo.m_magic = cur_undo_record.p->m_magic;
      *(cur_undo_record.p) = f_undo;

      LocalApply_undo_list undoList(c_apply_undo_pool,
                                    cur_undo_record_page.p->m_apply_undo_head);
      undoList.addLast(cur_undo_record);
      DEB_UNDO(("LDM(%u) FIRST WAIT page(%u,%u) count:%u lsn:%llu,"
                " data[%u,%u,%u], pending.p = %p",
                instance(),
                preq.m_page.m_file_no,
                preq.m_page.m_page_no,
                undoList.getCount(),
                f_undo.m_lsn,
                f_undo.m_data[3],
                f_undo.m_data[4],
                f_undo.m_data[5],
                cur_undo_record.p));

    }
    break; // Wait for callback
  case -1:
    ndbabort();
  default:
    ndbrequire(res > 0);
    DEB_UNDO(("LDM(%u) DIRECT_EXECUTE Page:%u lsn:%llu",
                        instance(),
                        preq.m_page.m_page_no,
                        f_undo.m_lsn));
    if (isNdbMtLqh())
    {
      jam();
      c_pending_undo_page_pool.release(cur_undo_record_page);
      // no page stored in hash, so i = RNIL
      preq.m_callback.m_callbackData = RNIL;
    }
    jam();
    /**
     * The m_immediate_flag variable stays false except for the time
     * from this call to execute until we reach the callback
     * where it is immediately read and immediately set back to
     * false again. Essentially this is a parameter to the
     * callback which is hard to get into the callback handling.
     */
    m_immediate_flag = true;
    execute(signal, preq.m_callback, res); // run callback
  }
}

void
Dbtup::disk_restart_undo_next(Signal* signal, Uint32 applied, Uint32 count_pending)
{
  signal->theData[0] = LgmanContinueB::EXECUTE_UNDO_RECORD;
  /* Flag indicating whether UNDO log was applied. */
  signal->theData[1] = applied;
  signal->theData[2] = count_pending;
  sendSignal(LGMAN_REF, GSN_CONTINUEB, signal, 3, JBB);
}

/**
 * This method is called before the UNDO log execution. It is called with
 * lcpId == RNIL when no LCP exists. It is called with the lcpId to restore
 * the fragment with when called with a value other than RNIL.
 */
void
Dbtup::disk_restart_lcp_id(Uint32 tableId,
                           Uint32 fragId,
                           Uint32 lcpId,
                           Uint32 localLcpId)
{
  /**
   * disk_restart_lcp_id is called from DBLQH when the restore of a
   * fragment is completed. At this time we know exactly which
   * lcpId that this fragment should use in its restore.
   * If no LCP is used to restore then lcpId is RNIL.
   */
  if (lcpId == RNIL)
  {
    jam();
    disk_restart_undo_lcp(tableId, fragId, Fragrecord::UC_NO_LCP, 0, 0, 0);
    DEB_UNDO_LCP(("(%u)mark_no_lcp tab(%u,%u), UC_NO_LCP",
                  instance(),
                  tableId,
                  fragId));
  }
  else
  {
    jam();
    disk_restart_undo_lcp(tableId,
                          fragId,
                          Fragrecord::UC_SET_LCP,
                          lcpId,
                          localLcpId,
                          0); 
    DEB_UNDO_LCP(("(%u)mark_no_lcp tab(%u,%u), UC_SET_LCP, LCP(%u,%u)",
                  instance(),
                  tableId,
                  fragId,
                  lcpId,
                  localLcpId));
  }
}

void
Dbtup::disk_restart_undo_lcp(Uint32 tableId,
                             Uint32 fragId,
                             Uint32 flag, 
			     Uint32 lcpId,
                             Uint32 localLcpId,
                             Uint32 lsn)
{
  Ptr<Tablerec> tabPtr;
  tabPtr.i= tableId;
  ptrCheckGuard(tabPtr, cnoOfTablerec, tablerec);

  if (tabPtr.p->tableStatus == DEFINED &&
      tabPtr.p->m_no_of_real_disk_attributes)
  {
    jam();
    FragrecordPtr fragPtr;
    getFragmentrec(fragPtr, fragId, tabPtr.i);
    if (!fragPtr.isNull())
    {
      jam();
      DEB_UNDO_LCP(("(%u)tab(%u,%u), lcp(%u,%u), flag: %u,"
                    " Fragment restore LCP(%u,%u), complete: %u",
                    instance(),
                    tableId,
                    fragId,
                    lcpId,
                    localLcpId,
                    flag,
                    fragPtr.p->m_restore_lcp_id,
                    fragPtr.p->m_restore_local_lcp_id,
                    fragPtr.p->m_undo_complete));
      switch(flag){
      case Fragrecord::UC_DROP:
      {
        jam();
        /**
         * In this case we have decided to start with a table.
         * If the table was dropped it must have been another table
         * that was dropped. Given that UNDO_TUP_CREATE isn't
         * logged we can find this at times. We should not look
         * any more at log records from this table going backwards
         * since they are belonging to an old table.
         */
	fragPtr.p->m_undo_complete = Fragrecord::UC_CREATE;
        return;
      }
      case Fragrecord::UC_CREATE:
      {
        /**
         * We have reached a point in the undo log record where the table
         * was created. This is not always inserted, but we don't perform
         * any UNDO operations after this operation have been seen.
         */
        jam();
	fragPtr.p->m_undo_complete = Fragrecord::UC_CREATE;
	return;
      }
      case Fragrecord::UC_NO_LCP:
      {
        jam();
        /**
         * We are restoring a table that had no LCPs connected to it.
         * We need to run the UNDO log for this table all the way back
         * to the table creation. We don't track table creations in the
         * UNDO log, so we have to execute the UNDO log back to the
         * LCP before it was created.
         */
	fragPtr.p->m_undo_complete = Fragrecord::UC_NO_LCP;
        return;
      }
      case Fragrecord::UC_LCP:
	jam();
        if (fragPtr.p->m_undo_complete == 0 &&
            fragPtr.p->m_restore_lcp_id == lcpId &&
            fragPtr.p->m_restore_local_lcp_id == localLcpId)
	{
	  jam();
          /**
           * We have reached the LCP UNDO log record, this indicates that the
           * fragment is now rolled back to where it should be.
           * We might still need to execute UNDO log record to synchronize the
           * page information with the extent bits.
           */
	  fragPtr.p->m_undo_complete = flag;
          DEB_UNDO_LCP(("(%u)tab(%u,%u) lcp(%u,%u) -> done, lsn=%u",
                        instance(),
                        tableId,
                        fragId,
                        lcpId,
                        localLcpId,
                        lsn));
	}
	return;
      case Fragrecord::UC_SET_LCP:
      {
	jam();
        /**
         * Used before UNDO log execution starts to set
         * m_restore_lcp_id for the fragment.
         */
        DEB_UNDO_LCP(("(%u)table(%u,%u) restore to lcp(%u,%u)",
                      instance(),
                      tableId,
                      fragId,
                      lcpId,
                      localLcpId));
	ndbrequire(fragPtr.p->m_undo_complete == 0);
	ndbrequire(fragPtr.p->m_restore_lcp_id == RNIL);
	fragPtr.p->m_restore_lcp_id = lcpId;
        fragPtr.p->m_restore_local_lcp_id = localLcpId;
	return;
      }
      }
      jamLine(flag);
      ndbabort();
    }
    else
    {
      DEB_UNDO_LCP(("(%u)table(%u,%u) No fragment found",
                    instance(),
                    tableId,
                    fragId));
    }
  }
  else
  {
    DEB_UNDO_LCP(("(%u)table(%u,%u) tabStatus: %u, disk: %u",
                  instance(),
                  tableId,
                  fragId,
                  tabPtr.p->tableStatus,
                  tabPtr.p->m_no_of_real_disk_attributes));
  }
}

void
Dbtup::release_undo_record(Ptr<Apply_undo>& undo_record, bool pending)
{
  if (pending)
  {
    jam();
    c_apply_undo_pool.release(undo_record);
  }
}

/**
 * Algorithm for applying undo records:
 *
 * The page_i passed is searched in the hashmap. If it is present,
 * it means there are pending undo records for the page, and they are processed
 * one by one from the list.
 * If it isn't present, the current undo record being processed in this signal
 * execution is the one which should be applied (f_undo).
 */
void
Dbtup::disk_restart_undo_callback(Signal* signal,
				  Uint32 page_i,
				  Uint32 page_id)
{
  jamEntry();
  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  PagePtr pagePtr;
  pagePtr.i = gpage.i;
  pagePtr.p = reinterpret_cast<Page*>(gpage.p);
  bool immediate_flag = m_immediate_flag;
  m_immediate_flag = false;
  Pending_undo_page* pendingPage = NULL;
  Apply_undo* undo = &f_undo;
  Uint32 count_pending = 1;

  bool pending = false;

  if (isNdbMtLqh())
  {
    jam();
    pending = (page_i != RNIL);

    if (pending)
    {
      jam();
      pendingPage = c_pending_undo_page_hash.getPtr(page_i);
      // page has outstanding undo records
      LocalApply_undo_list undoList(c_apply_undo_pool,
                                    pendingPage->m_apply_undo_head);
      count_pending = undoList.getCount();
      Tup_fixsize_page *fix_page = (Tup_fixsize_page*)pagePtr.p;
      (void)fix_page;
      DEB_UNDO(("LDM(%u) EXECUTE LIST CALLBACK page(%u,%u) on_page(%u,%u)"
                " tab(%u,%u) count:%u",
                instance(),
                pendingPage->m_file_no,
                pendingPage->m_page_no,
                fix_page->m_file_no,
                fix_page->m_page_no,
                fix_page->m_table_id,
                fix_page->m_fragment_id,
                count_pending));
    }
    else
    {
      DEB_UNDO(("LDM(%u) PAGE_NOT_FOUND_HASH", instance()));
    }
  }

  /**
   * Before we apply the UNDO record we need to discover which table
   * the page belongs to. For most pages this is listed in the page
   * header. However we cannot trust the page header since we could
   * come here with an UNDO log record for a page that have not ever
   * been written to disk after table creation. Worse the table could
   * even be listed as belonging to a different table and thus we
   * would create a mess here.
   *
   * To get the true identity of the page we will look up the table
   * in tsman, from this we will get the table id and fragment id
   * of the extent and this will also be the table id and fragment
   * id of the page we're dealing with here.
   *
   * Two things could happen here. We could come here with a page
   * that is belonging to table RNIL, this means that the page
   * was allocated after start of the LCP and also the extent was
   * allocated after the start of the LCP. In this case we don't
   * need to do anything, the extent isn't allocated to any table
   * and thus should remain a free extent and thus it doesn't make
   * sense to write to the page anything.
   *
   * Another variant is that the page belongs to a table which
   * isn't part of the restart, this can happen if the table
   * was dropped just before the crash.
   * Also in this case there is no need to do anything.
   *
   * Finally if we find that it belongs to an existing table, then
   * we will use this table id and fragment id here.
   *
   * Now the next question is if the page have been initialised
   * yet. We need to check 3 header variables for this.
   * table id, fragment id and table version.
   * Table id and fragment id isn't enough, the page could have belonged
   * a table with the same table id and fragment id, but it cannot at the
   * same time also have the same table version.
   *
   * Actually older versions didn't set the table version in the pages.
   * So it isn't possible here to be fully certain that the page belongs
   * to the correct table.
   *
   * A simple optimisation here is that this only needs to be done for
   * pages that misses in the page cache. If they are already in the page
   * cache then we can use the table id and fragment id as found in the
   * page header.
   *
   * For all pages that are changed or read into the page cache we will
   * also synchronize the extent bits with the page information.
   */

  if (! (pagePtr.p->list_index & 0x8000) ||
      pagePtr.p->nextList != RNIL ||
      pagePtr.p->prevList != RNIL)
  {
    jam();
    pagePtr.p->list_index |= 0x8000;
    pagePtr.p->nextList = pagePtr.p->prevList = RNIL;
#ifdef DEBUG_EXTENT_BITS
    Uint64 lsn = 0;
    lsn += pagePtr.p->m_page_header.m_page_lsn_hi;
    lsn <<= 32;
    lsn += pagePtr.p->m_page_header.m_page_lsn_lo;
    DEB_EXTENT_BITS(("(%u)Set list_index bit 0x8000 on page(%u,%u)"
                     " when undo, page_lsn = %llu, key(%u,%u).%u"
                     ", undo_lsn: %llu",
                     instance(),
                     pagePtr.p->m_file_no,
                     pagePtr.p->m_page_no,
                     lsn,
                     undo->m_key.m_file_no,
                     undo->m_key.m_page_no,
                     undo->m_key.m_page_idx,
                     undo->m_lsn));
#endif
  }
  
  Uint32 tableId= pagePtr.p->m_table_id;
  Uint32 fragId = pagePtr.p->m_fragment_id;
  Uint32 applied = 0;

  if (!pending) // direct execute, page not present in hash table.
  {
    ndbrequire(count_pending == 1);
  }

  for (Uint32 i = 1; i <= count_pending; i++)
  {
    Ptr<Apply_undo> pending_undo;
    if (pending)
    {
      jam();
      //Remove, process, release all Apply_undo from the list.
      LocalApply_undo_list undoList(c_apply_undo_pool,
                                    pendingPage->m_apply_undo_head);
      undoList.removeFirst(pending_undo);
      undo = pending_undo.p;
      undo->m_ptr = &undo->m_data[0];
      DEB_UNDO(("(%u) Execute pending data[%u,%u,%u], lsn: %llu,"
                " pending.p = %p",
                instance(),
                undo->m_data[3],
                undo->m_data[4],
                undo->m_data[5],
                undo->m_lsn,
                pending_undo.p));
    }

    /**
     * Ensure that the Page entry in PGMAN has the correct table id
     * fragment id set if it will be used in a future LCP.
     */
    Page_cache_client::Request preq;
    preq.m_page.m_file_no = undo->m_key.m_file_no;
    preq.m_page.m_page_no = undo->m_key.m_page_no;
    preq.m_table_id = tableId;
    preq.m_fragment_id = fragId;
    Page_cache_client pgman(this, c_pgman);
    ndbrequire(pgman.init_page_entry(preq));
  
    // process the undo record/s
    if (tableId >= cnoOfTablerec)
    {
      jam();
      DEB_UNDO(("(%u)UNDO table> %u, page(%u,%u).%u",
               instance(),
               tableId,
               undo->m_key.m_file_no,
               undo->m_key.m_page_no,
               undo->m_key.m_page_idx));
      release_undo_record(pending_undo, pending);
      continue;
    }
  
    undo->m_table_ptr.i = tableId;
    ptrCheckGuard(undo->m_table_ptr, cnoOfTablerec, tablerec);
    
    if (! (undo->m_table_ptr.p->tableStatus == DEFINED &&
           undo->m_table_ptr.p->m_no_of_real_disk_attributes))
    {
      jam();
      DEB_UNDO(("(%u)UNDO !defined (%u) on page(%u,%u).%u",
                instance(),
                tableId,
                undo->m_key.m_file_no,
                undo->m_key.m_page_no,
                undo->m_key.m_page_idx));
      release_undo_record(pending_undo, pending);
      continue;
    }

    Uint32 create_table_version = pagePtr.p->m_create_table_version;
    Uint32 page_version = pagePtr.p->m_ndb_version;

    ndbrequire(page_version >= NDB_DISK_V2);
    if (create_table_version !=
          c_lqh->getCreateSchemaVersion(tableId))
    {
      jam();
      DEB_UNDO(("UNDO fragment null %u/%u, old,new=(%u,%u), page(%u,%u).%u",
                 tableId,
                 fragId,
                 create_table_version,
                 c_lqh->getCreateSchemaVersion(tableId),
                 undo->m_key.m_file_no,
                 undo->m_key.m_page_no,
                 undo->m_key.m_page_idx));
      release_undo_record(pending_undo, pending);
      continue;
    }

    getFragmentrec(undo->m_fragment_ptr, fragId, undo->m_table_ptr.i);
    if (undo->m_fragment_ptr.isNull())
    {
      jam();
      DEB_UNDO(("(%u)UNDO fragment null tab(%u,%u), page(%u,%u).%u",
                instance(),
                tableId,
                fragId,
                undo->m_key.m_file_no,
                undo->m_key.m_page_no,
                undo->m_key.m_page_idx));
      release_undo_record(pending_undo, pending);
      continue;
    }

    Uint64 lsn = 0;
    applied = 0;
    lsn += pagePtr.p->m_page_header.m_page_lsn_hi;
    lsn <<= 32;
    lsn += pagePtr.p->m_page_header.m_page_lsn_lo;

    undo->m_page_ptr = pagePtr;

    if (undo->m_lsn <= lsn &&
        !undo->m_fragment_ptr.p->m_undo_complete)
    {
      jam();

      applied = applied | 1;
      /**
       * Apply undo record
       */
      switch(undo->m_type){
      case File_formats::Undofile::UNDO_TUP_ALLOC:
      {
        jam();
        disk_restart_undo_alloc(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_UPDATE:
      {
        jam();
        disk_restart_undo_update(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_FIRST_UPDATE_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = false;
        disk_restart_undo_update_first_part(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_FIRST_UPDATE_VAR_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = true;
        disk_restart_undo_update_first_part(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_UPDATE_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = true;
        disk_restart_undo_update_part(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_UPDATE_VAR_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = false;
        disk_restart_undo_update_part(undo);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_FREE:
      {
        jam();
        disk_restart_undo_free(undo, true, false);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_FREE_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = false;
        disk_restart_undo_free(undo, false, false);
        break;
      }
      case File_formats::Undofile::UNDO_TUP_FREE_VAR_PART:
      {
        jam();
        undo->m_in_intermediate_log_record = true;
        disk_restart_undo_free(undo, false, true);
        break;
      }
      default:
        ndbabort();
      }

      if ((undo->m_type != File_formats::Undofile::UNDO_TUP_UPDATE_PART) &&
          (undo->m_type != File_formats::Undofile::UNDO_TUP_UPDATE_VAR_PART))
      {
        jam();
        lsn = undo->m_lsn - 1; // make sure undo isn't run again...
        Page_cache_client pgman(this, c_pgman);
        pgman.update_lsn(signal, undo->m_key, lsn);
        jamEntry();
        disk_restart_undo_page_bits(signal, undo);
      }
    }
    else
    {
      jam();
      if (!immediate_flag &&
          undo->m_fragment_ptr.p->m_undo_complete != Fragrecord::UC_CREATE)
      {
        jam();
        /**
         * See Lemma 1 and Lemma 2 in analysis of extent page
         * synchronisation at restart.
         *
         * We don't need to call this function when immediate
         * flag since we already applied the first UNDO log
         * record on the page, there is no need to update
         * the page bits and the first log record have ensured
         * that the extent information is already allocated
         * properly.
         *
         * Also we don't go back from when a table was dropped or
         * created since we are then in territory where an old
         * incarnation of the table was and we need not handle
         * those log records.
         */
        DEB_UNDO(("(%u)disk_restart_undo_page_bits: page_lsn: %llu"
                  ", undo_lsn: %llu, page(%u,%u).%u",
                  instance(),
                  lsn,
                  undo->m_lsn,
                  undo->m_key.m_file_no,
                  undo->m_key.m_page_no,
                  undo->m_key.m_page_idx));
        disk_restart_undo_page_bits(signal, undo);
      }
      else
      {
        DEB_UNDO(("(%u)UNDO ignored: page_lsn: %llu"
                  ", undo_lsn: %llu, page(%u,%u).%u",
                  instance(),
                  lsn,
                  undo->m_lsn,
                  undo->m_key.m_file_no,
                  undo->m_key.m_page_no,
                  undo->m_key.m_page_idx));
      }
    }
    release_undo_record(pending_undo, pending);
  }

  ndbassert(count_pending != 0);
  if (isNdbMtLqh() && pending)
  {
    jam();
    LocalApply_undo_list undoList(c_apply_undo_pool,
                                  pendingPage->m_apply_undo_head);
    DEB_UNDO(("LDM(%u) Page:%u CheckCount:%u Applied:%u", instance(),
        pendingPage->m_page_no, undoList.getCount(), count_pending));
    ndbrequire(undoList.getCount() == 0);
    c_pending_undo_page_hash.remove(page_i);
    Ptr<Pending_undo_page> rel;
    rel.p = pendingPage;
    rel.i = page_i;
    c_pending_undo_page_pool.release(rel);
  }
  disk_restart_undo_next(signal, applied, count_pending);
}

void
Dbtup::disk_restart_undo_alloc(Apply_undo* undo)
{
#ifdef DEBUG_UNDO_ALLOC
  Uint64 lsn = 0;
  lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_hi;
  lsn <<= 32;
  lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_lo;
  DEB_UNDO_ALLOC(("(%u)applying %lld UNDO_TUP_ALLOC on page(%u,%u).%u"
                  ", page_lsn: %llu, tab(%u,%u), flag: %u",
                  instance(),
                  undo->m_lsn,
                  undo->m_key.m_file_no,
                  undo->m_key.m_page_no,
                  undo->m_key.m_page_idx,
                  lsn,
                  undo->m_fragment_ptr.p->fragTableId,
                  undo->m_fragment_ptr.p->fragmentId,
                  undo->m_fragment_ptr.p->m_undo_complete));
#endif
  ndbassert(undo->m_page_ptr.p->m_file_no == undo->m_key.m_file_no);
  ndbassert(undo->m_page_ptr.p->m_page_no == undo->m_key.m_page_no);
  if ((undo->m_table_ptr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    ((Fix_page*)undo->m_page_ptr.p)->free_record(undo->m_key.m_page_idx);
  }
  else
  {
    ((Var_page*)undo->m_page_ptr.p)->free_record(undo->m_key.m_page_idx, 0);
  }
}

Uint32*
Dbtup::prepare_undo_varpage(Var_page* page_ptr,
                            Uint32 idx,
                            Uint32 len)
{
  Uint32 entry_len = page_ptr->get_entry_len(idx);
  if (entry_len > len)
  {
    jam();
    /**
     * The entry_len is larger than the new length, thus it is ok to simply
     * decrease the size of the entry length and then simply copy the
     * data and be done with it.
     */
    page_ptr->shrink_entry(idx, len);
  }
  else if (entry_len < len)
  {
    jam();
    /**
     * The new entry is larger than what is available in the current entry.
     * We will release the record and immediately allocate a new one with
     * the same index. This ensures that we can perform any reorg of the
     * page if it is needed.
     */
    page_ptr->free_record(idx, 0);
    Uint32 new_idx = page_ptr->alloc_record(idx, len, (Var_page*)ctemp_page);
    ndbrequire(new_idx == idx);
  }
  return page_ptr->get_ptr(idx);
}
void
Dbtup::disk_restart_undo_update(Apply_undo* undo)
{
  Uint32* ptr;
  Uint32 len= undo->m_len - 4;
#ifdef DEBUG_UNDO
  {
  const Disk_undo::Update_Free *update =
    (const Disk_undo::Update_Free*)undo->m_ptr;
  const Uint32* src= update->m_data;
  Uint64 lsn = 0;
  lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_hi;
  lsn <<= 32;
  lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_lo;
  DEB_UNDO(("(%u)applying %lld UNDO_TUP_UPDATE on page(%u,%u).%u,"
            " page_lsn: %llu, data[%u,%u]",
            instance(),
            undo->m_lsn,
            undo->m_key.m_file_no,
            undo->m_key.m_page_no,
            undo->m_key.m_page_idx,
            lsn,
            src[0],
            src[1]));
  }
#endif
  if ((undo->m_table_ptr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    ptr= ((Fix_page*)undo->m_page_ptr.p)->get_ptr(undo->m_key.m_page_idx, len);
    ndbrequire(len == undo->m_table_ptr.p->m_offsets[DD].m_fix_header_size);
  }
  else
  {
    Uint32 idx = undo->m_key.m_page_idx;
    Var_page *page_ptr = (Var_page*)undo->m_page_ptr.p;
    ptr = prepare_undo_varpage(page_ptr, idx, len);
  }

  const Disk_undo::Update_Free *update =
    (const Disk_undo::Update_Free*)undo->m_ptr;
  const Uint32* src= update->m_data;
  ndbrequire(src[1] < Tup_page::DATA_WORDS);
  memcpy(ptr, src, 4 * len);
}

void
Dbtup::disk_restart_undo_update_first_part(Apply_undo* undo)
{
  Uint32* ptr;
  Uint32 header_len = 5;
  if (!undo->m_in_intermediate_log_record)
  {
    /* UNDO_TUP_FIRST_UPDATE_PART, old uses only 4 words in header */
    header_len = 4;
  }
  Uint32 len = undo->m_len - header_len;

  {
#ifdef DEBUG_UNDO
    const Disk_undo::Update_Free *update =
      (const Disk_undo::Update_Free*)undo->m_ptr;
    const Uint32* src= update->m_data;
    DEB_UNDO(("(%u)applying %lld UNDO_TUP_FIRST_UPDATE_PART"
              " on page(%u,%u).%u[%u], data[%x,%x], tot_len: %u"
              ", len: %u",
              instance(),
              undo->m_lsn,
              undo->m_key.m_file_no,
              undo->m_key.m_page_no,
              undo->m_key.m_page_idx,
              undo->m_offset,
              undo->m_in_intermediate_log_record ? src[1] : src[0],
              undo->m_in_intermediate_log_record ? src[2] : src[1],
              undo->m_tot_len,
              len));
#endif
  }

  /**
   * Variable sized disk data uses a different struct to write data and
   * thus must also calculate the pointer to the data differently to
   * the fixed size row handling.
   */
  Uint32 *src;
  if ((undo->m_table_ptr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    Disk_undo::Update_Free *update =
      (Disk_undo::Update_Free*)undo->m_ptr;
    src = update->m_data;
    ptr= ((Fix_page*)undo->m_page_ptr.p)->get_ptr(undo->m_key.m_page_idx, len);
    ndbrequire(len < undo->m_table_ptr.p->m_offsets[DD].m_fix_header_size);
  }
  else
  {
    Disk_undo::Update_Free_FirstVarPart *update =
      (Disk_undo::Update_Free_FirstVarPart*)undo->m_ptr;
    src = update->m_data;
    Uint32 idx = undo->m_key.m_page_idx;
    Var_page *page_ptr = (Var_page*)undo->m_page_ptr.p;
    ptr = prepare_undo_varpage(page_ptr,
                               idx,
                               undo->m_tot_len);
  }
  ndbrequire(len < 2 || src[1] < Tup_page::DATA_WORDS);
#ifdef DEBUG_UNDO_SPLIT
  log_buf_print(src, len);
#endif
  memcpy(ptr, src, 4 * len);
}

void
Dbtup::disk_restart_undo_update_part(Apply_undo* undo)
{
  Uint32* ptr;
  Uint32 len= undo->m_len - 5;

  DEB_UNDO(("(%u)applying %lld UNDO_TUP_UPDATE_PART on page(%u,%u).%u[%u]"
            ", len: %u",
            instance(),
            undo->m_lsn,
            undo->m_key.m_file_no,
            undo->m_key.m_page_no,
            undo->m_key.m_page_idx,
            undo->m_offset,
            len));

  Uint32 offset = undo->m_offset;
  if ((undo->m_table_ptr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    Uint32 fix_header_size = undo->m_table_ptr.p->m_offsets[DD].m_fix_header_size;
    ptr= ((Fix_page*)undo->m_page_ptr.p)->get_ptr(undo->m_key.m_page_idx, len);
    ndbrequire((len + offset) == fix_header_size);
  }
  else
  {
    Uint32 idx = undo->m_key.m_page_idx;
    Var_page *page_ptr = (Var_page*)undo->m_page_ptr.p;
    ptr = page_ptr->get_ptr(idx);
    Uint32 tot_len = page_ptr->get_entry_len(idx);
    ndbrequire(tot_len == (offset + len));
  }
  ptr = &ptr[offset];

  const Disk_undo::UpdatePart *update = (const Disk_undo::UpdatePart*)undo->m_ptr;
  const Uint32* src= update->m_data;
  ndbrequire(undo->m_offset != 0 ||
             src[1] < Tup_page::DATA_WORDS);
#ifdef DEBUG_UNDO_SPLIT
  log_buf_print(src, len);
#endif
  memcpy(ptr, src, 4 * len);
}

void
Dbtup::disk_restart_undo_free(Apply_undo* undo,
                              bool full_free,
                              bool varsized_part)
{
  Uint32* ptr, idx = undo->m_key.m_page_idx;
  Uint32 header_len = varsized_part ? 5 : 4;
  Uint32 len= undo->m_len - header_len;
#ifdef DEBUG_UNDO_ALLOC
  {
    Uint64 lsn = 0;
    lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_hi;
    lsn <<= 32;
    lsn += undo->m_page_ptr.p->m_page_header.m_page_lsn_lo;
    const char *free_str = (const char*)"UNDO_TUP_FREE";
    const char *free_part_str = (const char*)"UNDO_TUP_FREE_PART";
    const char *free_var_part_str = (const char*)"UNDO_TUP_FREE_VAR_PART";
    Uint32 *src;
    if (!varsized_part)
    {
      Disk_undo::Update_Free *free =
        (Disk_undo::Update_Free*)undo->m_ptr;
      src= free->m_data;
    }
    else
    {
      Disk_undo::Update_Free_FirstVarPart *free =
        (Disk_undo::Update_Free_FirstVarPart*)undo->m_ptr;
      src= free->m_data;
    }
    DEB_UNDO_ALLOC(("(%u)applying %lld %s on page(%u,%u).%u, page_lsn:"
                    " %llu idx:%u, tab(%u,%u), flag: %u,"
                    " data[%u,%u], len: %u, ptr: %p",
                    instance(),
                    undo->m_lsn,
                    full_free ?
                      free_str : varsized_part ?
                        free_var_part_str : free_part_str,
                    undo->m_key.m_file_no,
                    undo->m_key.m_page_no,
                    undo->m_key.m_page_idx,
                    lsn,
                    idx,
                    undo->m_fragment_ptr.p->fragTableId,
                    undo->m_fragment_ptr.p->fragmentId,
                    undo->m_fragment_ptr.p->m_undo_complete,
                    src[0],
                    src[1],
                    len,
                    src));
  }
#endif
  if ((undo->m_table_ptr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    idx= ((Fix_page*)undo->m_page_ptr.p)->alloc_record(idx);
    Uint32 fix_header_size = undo->m_table_ptr.p->m_offsets[DD].m_fix_header_size;
    ndbrequire(!varsized_part);
    if (full_free)
    {
      ndbrequire(len == fix_header_size);
    }
    else
    {
      ndbrequire(len < fix_header_size);
    }
    ptr= ((Fix_page*)undo->m_page_ptr.p)->get_ptr(idx, fix_header_size);
  }
  else
  {
    Var_page *page_ptr = (Var_page*)undo->m_page_ptr.p;
    Uint32 alloc_len = len;
    if (varsized_part)
    {
      jam();
      alloc_len = undo->m_tot_len;
    }
    Uint32 new_idx= page_ptr->alloc_record(idx,
                                           alloc_len,
                                           (Var_page*)ctemp_page);
    ndbrequire(new_idx == idx);
    ptr = page_ptr->get_ptr(idx);
  }

  if (idx != undo->m_key.m_page_idx)
  {
    Uint64 lsn = undo->m_lsn;
    jam();
    jamLine(lsn & 0xFFFF);
    jamLine((lsn >> 16) & 0xFFFF);
    jamLine((lsn >> 32) & 0xFFFF);
    jamLine((lsn >> 48) & 0xFFFF);
    ndbabort();
  }
  Uint32 *src;
  if (!varsized_part)
  {
    Disk_undo::Update_Free *free =
      (Disk_undo::Update_Free*)undo->m_ptr;
    src= free->m_data;
  }
  else
  {
    Disk_undo::Update_Free_FirstVarPart *free =
      (Disk_undo::Update_Free_FirstVarPart*)undo->m_ptr;
    src= free->m_data;
  }
  ndbrequire(src[1] < Tup_page::DATA_WORDS);
  memcpy(ptr, src, 4 * len);
}

void
Dbtup::disk_restart_undo_page_bits(Signal* signal, Apply_undo* undo)
{
  Fragrecord* fragPtrP = undo->m_fragment_ptr.p;
  Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;
  
  /**
   * Set alloc.m_curr_extent_info_ptr_i to
   *   current this extent (and move old extend into free matrix)
   */
  Page* pageP = undo->m_page_ptr.p;
  Uint32 free = pageP->free_space;
  Uint32 new_idx = alloc.calc_page_free_bits(free);
  pageP->list_index = 0x8000 | new_idx;

  D("Tablespace_client - disk_restart_undo_page_bits");
  Tablespace_client tsman(signal, this, c_tsman,
			  fragPtrP->fragTableId,
	 		  fragPtrP->fragmentId,
                          c_lqh->getCreateSchemaVersion(fragPtrP->fragTableId),
			  fragPtrP->m_tablespace_id);

  DEB_EXTENT_BITS(("(%u)tab(%u,%u), page(%u,%u):%u new_idx: %u,"
                   " free_space: %u, page_tab(%u,%u).%u",
                  instance(),
                  fragPtrP->fragTableId,
                  fragPtrP->fragmentId,
                  pageP->m_file_no,
                  pageP->m_page_no,
                  undo->m_page_ptr.i,
                  new_idx,
                  free,
                  pageP->m_table_id,
                  pageP->m_fragment_id,
                  pageP->m_create_table_version));

  tsman.restart_undo_page_free_bits(&undo->m_key, new_idx);
  jamEntry();
}

/**
 * disk_restart_alloc_extent is called during scan of extent
 * headers in TSMAN. It ensures that we build the extent data
 * structures that ensures that we select the proper extent for
 * new records.
 *
 * The data to build is to start with the Extent_info struct.
 * m_free_space
 * ------------
 * This variable contains the number free records available
 * in the extent. It is initialised to
 * number of pages in extent times the number of records per
 * page when creating a new extent. Each prealloc will
 * decrease the number by one and each free will increase it
 * by one (also abort of prealloc).
 * At restarts we don't know the number so it is first set to
 * 0. Next it is set according to the page bits in the extent
 * information stored on disk by TSMAN.
 *
 * The page bits on disk have the following meaning:
 * 0: The page is free, no records stored there
 * 1: The page is not free and not full, at least one record
 *    is stored in the page.
 * 2: The page is full
 * 3: The page is full
 *
 * The meaning is a bit different for variable sized pages, see
 * m_dirty_pages below.
 *
 * For free pages we add number of records per page, for "half full"
 * pages we add to number of free pages in extent.
 * This means that this number is a minimum of the actual number of
 * free records in the extent.
 * Each time we use a page we will check the m_restart_seq variable on
 * the page (not checked during UNDO log execution since the variables
 * are not initialised at that time). If it isn't set to the
 * current m_restart_seq it means that the page is not yet fully
 * known. In this case we will call restart_setup_page that will
 * update the m_free_pages variable correctly for the page and will
 * also update the extent position (explained below).
 *
 * m_free_page_count
 * -----------------
 * For each state above we have a count of how many pages of each type
 * that we have. When initialised we set all pages to be in free bucket.
 * At restart we set all counters to 0, next we check each page in the
 * call to disk_restart_page_bits, this is called immediately after
 * the call to disk_restart_alloc_extent for each page in the extent.
 *
 * m_empty_page_no
 * ---------------
 * This is only used the first time we create the extent. It is never
 * used after a node restart. It makes sure that we allocate free
 * pages from the beginning of the extent to the end of the extent.
 * The variable isn't really necessary since it will work fairly good
 * also after a restart.
 *
 * m_first_page_no
 * ---------------
 * This is the page number of the first page in the extent. This is the
 * page id in the data file, so page id 3 is the 3rd 32kByte page in the
 * data file.
 *
 * m_key
 * -----
 * This represents the information about the extent page and extent number.
 * m_key.m_file_no is the file number of the extent
 * m_key.m_page_no is the page number of the first page in the extent
 *
 * m_extent_no
 * -----------
 * m_extent_no is the extent number, can be used to find the exact place
 * of the extent information on the extent pages
 *
 * nextHash, prevHash
 * ------------------
 * Each extent is placed in a hash table c_extent_hash. The key to this
 * hash table is m_key above, the m_page_no is not part of the key. So
 * a key with m_file_no set to file number and m_page_idx set to
 * extent number will find the appropriate extent.
 *
 * nextPool
 * --------
 * Used for linking free extent records in the c_extent_pool.
 * When allocated it is used to keep things in the m_extent_list.
 *
 * nextList, prevList
 * ------------------
 * Used to store the extent information in one of the 20 lists
 * in m_free_extents in the Disk_alloc_info struct as part of
 * the fragment.
 * The general idea about this matrix is explained in the
 * paper "Recovery in MySQL Cluster 5.1" presented at
 * VLDB 2005.
 *
 * m_free_matrix_pos
 * -----------------
 * This specifies which of the 20 lists the extent is currently
 * stored in. If set to RNIL then it is the extent referred to
 * from the m_curr_extent_info_ptr_i in the Disk_alloc_info
 * struct of the fragment. This indicates the current extent
 * used to insert data into.
 *
 * The data structures in Disk_alloc_info is referring to extent
 * information.
 *
 * Disk_alloc_info data variables (part of fragment)
 * -------------------------------------------------
 *
 * m_extent_size
 * -------------
 * Size of the extents used by this fragment
 *
 * m_curr_extent_info_ptr_i
 * ------------------------
 * Pointing to the current extent used for inserts, RNIL if
 * no current one.
 *
 * m_free_extents
 * --------------
 * List of extents as arranged in a matrix, there are 20
 * entries in a 5,4 matrix.
 *
 * The row information is the free level.
 * Row 0 is at least 80% free
 * Row 1 is at least 60% free
 * Row 2 is at least 40% free
 * Row 3 is at least 20% free
 * Row 4 is at least 0% free
 *
 * Col is based on the states described above. So if any page
 * in extent is fully free it will be in column 0.
 * If at least one page in extent is in "half full" state it
 * will be in column 1, if any page is in full state 2 it will
 * be in column 2 and otherwise it will be in column 3.
 * Search starts in Row 0 and goes through the columns, next
 * to Row 1 and so forth.
 *
 * m_total_extent_free_space_thresholds
 * ------------------------------------
 * This variable is static after creating the fragment. It
 * provides the levels on number of records for 80% level,
 * 60% level and so forth.
 *
 * m_page_free_bits_map
 * --------------------
 * This is also static information after creation of fragment.
 * It describes the number of free records in a page when in
 * states 0 through.
 * In state 0 it is set to records per page.
 * State 1 is set to 1
 * State 2 and 3 is set to 0.
 *
 * m_extent_list
 * -------------
 * This list is used for disk scans. In this case we need to know all
 * disk pages and these are found by scanning all extents one by one.
 * New extents are added first, so new pages added during scan are not
 * seen by the scan. Disk scans are currently only used for backups.
 *
 * m_dirty_pages
 * -------------
 * These list has two purposes, the main purpose is to enable quick
 * finding of pages for new rows in disk_page_prealloc. The list is
 * also used at drop of the table where we will call drop_page on
 * each page in the lists.
 * 
 * There is one list per state. When allocating a new page for insert we
 * search for a page in the free (state 0) and "half full" (state 1)
 * lists. If any page is in these lists we're done with our search of
 * page to insert into. This happens in disk_page_prealloc.
 *
 * With variable sized pages we make use of all 4 lists, list 0 is
 * free, list 1 is less than half full, list 2 has at least one
 * sixth of the page free and list 3 means that less than one
 * sixth of the page is free.
 *
 * If a page is found in dirty pages we immediately update the
 * extent position of the page, we also move the page to another
 * list in m_dirty_pages if state changed due to insert, finally
 * we also update m_free_page_count above on the extent if state
 * changed.
 *
 * If the prealloc is aborted we remove the record from the page
 * and update the same structures again if necessary.
 *
 * When the page arrives from disk we also check whether there is a
 * need to change the m_free_page_count and extent position. A page
 * only arrives from disk after disk_page_prealloc if we were unable
 * to find a page among the ones already in memory that could fit the
 * new row. Here it is also placed in the proper m_dirty_pages list.
 * It is a new page at this point not currently in any list since it
 * comes from disk. It could actually come from the page cache still.
 * This could happen when a page have been read and is used for writing.
 * We don't use any knowledge of what pages have been read when
 * selecting which page to write.
 *
 * There are also some important variables on each page that is used
 * for page allocation.
 *
 * m_unmap_pages
 * -------------
 * Whenever a data page (not extent page) is to be flushed to disk PGMAN
 * will inform DBTUP about this. It will inform it before the flush and
 * also when the flush is completed.
 *
 * Before flush we will move the page away from the m_dirty_pages list
 * and into the m_unmap_pages list. If the dirty count is down to 0
 * we will also set list_index bit 0x8000 to indicate page is not in
 * dirty page list. We also set the uncommitted bits in the extent
 * information before we flush it to disk.
 *
 * After flush we will remove it from the unmap pages list.
 * We will also update the extent information if necessary and if it
 * has changed we will set the page to be dirty in PGMAN.
 *
 * m_page_requests
 * ---------------
 * This is a set of lists, one list for each state as described above.
 * Pages in these lists are in transit from disk to the memory to be
 * made dirty. Thus they are suitable to be used if no dirty pages are
 * available in memory. When we use those pages we will also move them
 * to the proper list to ensure that they are no longer used when already
 * full.
 *
 * list_index
 * ----------
 * This represents the state of the page from above (0 free, 1 "half full",
 * 2 and 3 full). Also if 0x8000 is set the page isn't in the m_dirty_pages
 * list.
 *
 * free_space
 * ----------
 * This is the count of the number of records stored on the page. It is
 * update by calls to free_record and alloc_record in tuppage.cpp.
 *
 * disk_page_prealloc
 * ------------------
 * This function is called to allocate a record for use in insert of disk
 * record. It returns the page id and page index of the row to be used.
 * The page isn't necessarily available in memory when returned from
 * this function. It is however guaranteed to at least be in transit
 * from disk. So the caller can safely call get_page on this page and
 * know that when it arrives it will be ready for consumption. The
 * callbacks are executed in order, so this means that
 * disk_page_prealloc_callback is called before the callback used by
 * the caller to actually perform the insert action.
 */
int
Dbtup::disk_restart_alloc_extent(EmulatedJamBuffer* jamBuf, 
                                 Uint32 tableId,
                                 Uint32 fragId,
                                 Uint32 create_table_version,
				 const Local_key* key,
                                 Uint32 extent_no,
                                 Uint32 pages)
{
  /**
   * This function is called from TSMAN in rep thread. Must not use any
   * block variables other than extent information.
   */
  TablerecPtr tabPtr;
  FragrecordPtr fragPtr;
  tabPtr.i = tableId;
  ptrCheckGuard(tabPtr, cnoOfTablerec, tablerec);
  Uint32 current_create_table_version = c_lqh->getCreateSchemaVersion(tableId);
  DEB_EXTENT_BITS(("(%u)disk_restart_alloc_extent: tab(%u,%u):%u,"
                   " current version: %u",
                   instance(),
                   tableId,
                   fragId,
                   create_table_version,
                   current_create_table_version));

  if (tabPtr.p->tableStatus == DEFINED &&
      tabPtr.p->m_no_of_real_disk_attributes &&
      (current_create_table_version == create_table_version ||
       create_table_version == 0))
  {
    thrjam(jamBuf);
    getFragmentrec(fragPtr, fragId, tabPtr.i);

    if (!fragPtr.isNull())
    {
      thrjam(jamBuf);

      Disk_alloc_info& alloc= fragPtr.p->m_disk_alloc_info;
      
      Ptr<Extent_info> ext;
      ndbrequire(c_extent_pool.seize(ext));
#ifdef VM_TRACE
      ndbout << "allocated " << pages << " pages: " << *key 
	     << " table: " << tabPtr.i << " fragment: " << fragId << endl;
#endif      
      ext.p->m_key = *key;
      ext.p->m_extent_no = extent_no;
      ext.p->m_first_page_no = ext.p->m_key.m_page_no;
      ext.p->m_free_space= 0;
      DEB_FREE_SPACE(("(%u)4 Set m_free_space: %u, extP: %p",
                      instance(),
                      ext.p->m_free_space,
                      ext.p));
      ext.p->m_empty_page_no = (1 << 16); // We don't know, so assume none
      DEB_EXTENT_BITS_HASH((
                "(%u)restart:extent(%u).%u in tab(%u,%u),"
                " first_page(%u,%u)",
                instance(),
                ext.p->m_extent_no,
                ext.i,
                fragPtr.p->fragTableId,
                fragPtr.p->fragmentId,
                ext.p->m_key.m_file_no,
                ext.p->m_first_page_no));
      memset(ext.p->m_free_page_count, 0, sizeof(ext.p->m_free_page_count));
      
      if (alloc.m_curr_extent_info_ptr_i != RNIL)
      {
        thrjam(jamBuf);
        Ptr<Extent_info> old;
        ndbrequire(c_extent_pool.getPtr(old, alloc.m_curr_extent_info_ptr_i));
        ndbassert(old.p->m_free_matrix_pos == RNIL);
        Uint32 pos= alloc.calc_extent_pos(old.p);
        Local_extent_info_list new_list(c_extent_pool, alloc.m_free_extents[pos]);
        new_list.addFirst(old);
        DEB_FREE_EXTENT(("(%p) 2:Add ext.p: %p from pos: %u",
                       &alloc, old.p, pos));
        old.p->m_free_matrix_pos= pos;
      }
      
      alloc.m_curr_extent_info_ptr_i = ext.i;
      ext.p->m_free_matrix_pos = RNIL;
      c_extent_hash.add(ext);

      Local_fragment_extent_list list1(c_extent_pool, alloc.m_extent_list);
      list1.addFirst(ext);
      return 0;
    }
  }
  thrjam(jamBuf);
  return -1;
}

/**
 * This function is called from TSMAN during scan of extent headers.
 * It is vital that the LDM thread is not doing any activity
 * regarding this information at the same time. This only happens
 * in a very specific part of restart. It is vital to ensure that
 * one only uses stack variables and no block variables. The only
 * block variables allowed to use are those that we update here, that
 * is the extent information of a fragment and this must not be
 * manipulated at the same time from LDM thread activity, this is
 * safe guarded by the restart phase serialisation.
 */
void
Dbtup::disk_restart_page_bits(EmulatedJamBuffer* jamBuf, 
                              Uint32 tableId,
                              Uint32 fragId,
                              Uint32 create_table_version,
			      const Local_key* key,
                              Uint32 bits)
{
  thrjam(jamBuf);
  TablerecPtr tabPtr;
  FragrecordPtr fragPtr;
  Uint32 current_create_table_version = c_lqh->getCreateSchemaVersion(tableId);
  tabPtr.i = tableId;
  ptrCheckGuard(tabPtr, cnoOfTablerec, tablerec);
  if (tabPtr.p->tableStatus == DEFINED &&
      tabPtr.p->m_no_of_real_disk_attributes &&
      (current_create_table_version == create_table_version ||
       create_table_version == 0))
  {
    thrjam(jamBuf);
    getFragmentrec(fragPtr, fragId, tabPtr.i);
    ndbrequire(fragPtr.i != RNIL64);
    Disk_alloc_info& alloc= fragPtr.p->m_disk_alloc_info;
    
    Ptr<Extent_info> ext;
    ndbrequire(c_extent_pool.getPtr(ext, alloc.m_curr_extent_info_ptr_i));
    
    Uint32 size= alloc.calc_page_free_space(bits);  
    
    ext.p->m_free_page_count[bits]++;
    DEB_EXTENT_BITS(("(%u)disk_restart_page_bits:extent(%u), tab(%u,%u),"
                     " page(%u,%u), bits: %u, new_count: %u",
                     instance(),
                     ext.p->m_extent_no,
                     tableId,
                     fragId,
                     key->m_file_no,
                     key->m_page_no,
                     bits,
                     ext.p->m_free_page_count[bits]));

    // actually only to update free_space
    DEB_FREE_SPACE(("(%u) update_extent_pos from %u page(%u,%u)",
                    instance(),
                    __LINE__,
                    key->m_file_no,
                    key->m_page_no));
    update_extent_pos(jamBuf,
                      fragPtr.p,
                      ext,
                      EXTENT_SEARCH_MATRIX_COLS - 1,
                      bits,
                      size);
    ndbassert(ext.p->m_free_matrix_pos == RNIL);
    DEB_EXTENT_BITS(("(%u)disk_restart_page_bits in tab(%u,%u):%u,"
                     " page(%u,%u), bits: %u, ext.i: %u,"
                     " extent_no: %u",
                     instance(),
                     tableId,
                     fragId,
                     create_table_version,
                     key->m_file_no,
                     key->m_page_no,
                     bits,
                     ext.i,
                     ext.p->m_extent_no));
  }
}

void
Dbtup::disk_page_get_allocated(const Tablerec* tabPtrP,
                               const Fragrecord * fragPtrP,
                               Uint64 res[2])
{
  res[0] = res[1] = 0;
  if (tabPtrP->m_no_of_disk_attributes)
  {
    jam();
    const Disk_alloc_info& alloc= fragPtrP->m_disk_alloc_info;
    Uint64 cnt = 0;
    Uint64 free = 0;

    {
      Disk_alloc_info& tmp = const_cast<Disk_alloc_info&>(alloc);
      Local_fragment_extent_list list(c_extent_pool, tmp.m_extent_list);
      free = alloc.m_tot_free_space;
      cnt = list.getCount();
    }
    free *= Uint64(tabPtrP->m_offsets[DD].m_fix_header_size);
    free *= Uint64(4);

    res[0] = cnt * alloc.m_extent_size * File_formats::NDB_PAGE_SIZE;
    res[1] = free;
  }
}
