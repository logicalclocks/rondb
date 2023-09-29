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

#ifndef NDB_POOL_HPP
#define NDB_POOL_HPP

#include <climits>

#include <ndb_global.h>
#include <kernel_types.h>

#define JAM_FILE_ID 315


/**
 * Type bits
 *
 * Type id is 11 bits record type, and 5 bits resource id
 *   -> 2048 different kind of records and 32 different resource groups
 * 
 * Resource id is used to handle configuration parameters
 *
 * see blocks/records_types.hpp
 */
#define RG_BITS 5
#define RG_MASK ((1 << RG_BITS) - 1)
#define MAKE_TID(TID,RG) Uint32((TID << RG_BITS) | RG)
#define GET_RG(rt) (rt & RG_MASK)
#define GET_TID(rt) (rt >> RG_BITS)

/**
 * Page bits
 */
#define POOL_RECORD_BITS 13
#define POOL_RECORD_MASK ((1 << POOL_RECORD_BITS) - 1)

/**
 * Record_info
 *
 */
struct Record_info
{
  Uint16 m_size;
  Uint16 m_type_id;
  Uint16 m_offset_next_pool;
  Uint16 m_offset_magic;
};

/**
  Contains both restrictions and current state of a resource groups page memory
  usage.
 */

struct Resource_limit
{
  static constexpr Uint32 HIGHEST_LIMIT = UINT32_MAX;

  /**
    Minimal number of pages dedicated for the resource group from shared global
    page memory.

    If set to zero it also indicates that the resource group have lower
    priority than resource group with some dedicated pages.

    The lower priority denies the resource group to use the last percentage of
    shared global page memory.

    See documentation for Resource_limits.
  */
  Uint32 m_min;

  /**
    Maximal number of pages that the resource group may allocate from shared
    global page memory.

    If set to zero there is no restrictions caused by this member.
  */
  Uint32 m_max;

  /**
   * After reaching this number we are only allowed to request pages using a
   * lower priority.
   */
  Uint32 m_max_high_prio;

  /**
    Number of pages currently in use by resource group.
  */
  Uint32 m_curr;

  /**
    Number of pages currently reserved as spare.
    These pages are used by DataMemory as a way to ensure that we spare a bit
    of memory for restart situations, for situations where we want to add new
    fragments or reorganize fragments in some other fashion.
  */
  Uint32 m_spare;

  /**
   * We have run completely out of resources and stolen pages from some other
   * resource with reserved memory. To be able to keep the counters consistent
   * we need to ensure that these are returned as soon as possible.
   */
  Uint32 m_stolen_reserved;

  /**
   * We have used all our reserved resources, but calls to alloc_emergency_page
   * have extended our set of reserved pages. These pages are either allocated
   * from shared global memory or stolen from the reserved memory of another
   * Resource group.
   */
  Uint32 m_overflow_reserved;

  /**
    A positive number identifying the resource group.
  */
  Uint32 m_resource_id;

  /**
   * See explanation in Resource_limits class.
   */
  enum PrioMemory
  {
    LOW_PRIO_MEMORY = 0,
    HIGH_PRIO_MEMORY = 1,
    ULTRA_HIGH_PRIO_MEMORY = 2
  };
  PrioMemory m_prio_memory;
};

class Magic
{
public:
  explicit Magic(Uint32 type_id) { m_magic = make(type_id); }
  bool check(Uint32 type_id) { return match(m_magic, type_id); }
  template<typename T> static bool check_ptr(const T* ptr)
  {
    return match(ptr->m_magic, T::TYPE_ID);
  }
  template<typename T> static bool check_ptr_rw(const T* ptr)
  {
    return match_rw(ptr->m_magic, T::TYPE_ID);
  }
  static Uint32 make(Uint32 type_id);
  static bool match(Uint32 magic, Uint32 type_id);
  static Uint32 make_rw(Uint32 type_id);
  static bool match_rw(Uint32 magic, Uint32 type_id);
private:
  Uint32 m_magic;
};

inline Uint32 Magic::make(Uint32 type_id)
{
  return type_id ^ ((~type_id) << 16);
}

inline bool Magic::match(Uint32 magic, Uint32 type_id)
{
  return magic == make(type_id);
}

inline Uint32 Magic::make_rw(Uint32 type_id)
{
  return ~type_id;
}

inline bool Magic::match_rw(Uint32 magic, Uint32 type_id)
{
  return magic == make_rw(type_id);
}

class Ndbd_mem_manager;
struct Pool_context
{
  Pool_context() {}
  class SimulatedBlock* m_block;

  /**
   * Get mem root
   */
  void* get_memroot() const;
  Ndbd_mem_manager* get_mem_manager() const;

  /**
   * Alloc page.
   *
   *   @param[out] i  i value of first page
   *   @return     pointer to first page (NULL if failed)
   *
   * Will handle resource limit
   */
  void* alloc_page19(Uint32 type_id, Uint32 *i, bool allow_use_spare = false);
  void* alloc_page27(Uint32 type_id, Uint32 *i, bool allow_use_spare = false);
  void* alloc_page30(Uint32 type_id, Uint32 *i, bool allow_use_spare = false);
  void* alloc_page32(Uint32 type_id, Uint32 *i, bool allow_use_spare = false);

  /**
   * Release page
   *
   *   @param[in] i   i value of first page
   */
  void release_page(Uint32 type_id, Uint32 i);

  /**
   * Alloc consecutive pages
   *
   *   @param[in,out] cnt  no of requested pages,
   *                       return no of allocated (undefined return NULL)
   *                       out will never be > in
   *   @param[out] i  i value of first page
   *   @param[in] min will never allocate less than min
   *   @return        pointer to first page (NULL if failed)
   *
   * Will handle resource limit
   */
  void* alloc_pages(Uint32 type_id, Uint32 *i, Uint32 *cnt, Uint32 min =1);

  /**
   * Release pages
   *
   *   @param[in] i    i value of first page
   *   @param[in] cnt  no of pages to release
   */
  void release_pages(Uint32 type_id, Uint32 i, Uint32 cnt);

  void* get_valid_page(Uint32 page_num) const;

  /**
   * Abort
   */
  [[noreturn]] void handleAbort(int code, const char* msg) const;
};

template <typename T>
struct Ptr 
{
  typedef Uint32 I;
  T * p;
  Uint32 i;

  static Ptr get(T* _p, Uint32 _i) { Ptr x; x.p = _p; x.i = _i; return x; }

  /**
    Initialize to ffff.... in debug mode. The purpose of this is to detect
    use of uninitialized values by causing an error. To maximize performance,
    this is done in debug mode only (when asserts are enabled).
   */
  Ptr(){assert(memset(this, 0xff, sizeof(*this)));}
  Ptr(T* pVal, Uint32 iVal):p(pVal), i(iVal){}


  bool isNull() const 
  { 
    assert(i <= RNIL);
    return i == RNIL; 
  }

  inline void setNull()
  {
    i = RNIL;
  }
};

template <typename T>
struct ConstPtr 
{
  const T * p;
  Uint32 i;

  static ConstPtr get(T const* _p, Uint32 _i) { ConstPtr x; x.p = _p; x.i = _i; return x; }

  /**
    Initialize to ffff.... in debug mode. The purpose of this is to detect
    use of uninitialized values by causing an error. To maximize performance,
    this is done in debug mode only (when asserts are enabled).
   */
  ConstPtr(){assert(memset(this, 0xff, sizeof(*this)));}
  ConstPtr(T* pVal, Uint32 iVal):p(pVal), i(iVal){}

  bool isNull() const 
  { 
    assert(i <= RNIL);
    return i == RNIL; 
  }

  inline void setNull()
  {
    i = RNIL;
  }
};

template <typename T>
struct Ptr64
{
  typedef Uint64 I;
  T * p;
  Uint64 i;

  static Ptr64 get(T* _p, Uint64 _i)
  {
    Ptr64 x;
    x.p = _p;
    x.i = _i;
    return x;
  }

  Ptr64(){assert(memset(this, 0xff, sizeof(*this)));}
  Ptr64(T* pVal, Uint64 iVal):p(pVal), i(iVal){}


  bool isNull() const 
  { 
    assert(i <= RNIL64);
    return i == RNIL64; 
  }

  inline void setNull()
  {
    i = RNIL64;
  }
};

template <typename T>
struct ConstPtr64
{
  const T * p;
  Uint64 i;

  static ConstPtr64 get(T const* _p, Uint32 _i)
  {
    ConstPtr64 x;
    x.p = _p;
    x.i = _i;
    return x;
  }

  ConstPtr64(){assert(memset(this, 0xff, sizeof(*this)));}
  ConstPtr64(T* pVal, Uint64 iVal):p(pVal), i(iVal){}

  bool isNull() const 
  { 
    assert(i <= RNIL64);
    return i == RNIL64; 
  }

  inline void setNull()
  {
    i = RNIL64;
  }
};

#ifdef XX_DOCUMENTATION_XX
/**
 * Any pool should implement the following
 */
struct PoolImpl
{
  Pool_context m_ctx;
  Record_info m_record_info;
  
  void init(const Record_info& ri, const Pool_context& pc);
  void init(const Record_info& ri, const Pool_context& pc);
  
  bool seize(Ptr<void>&);
  void release(Ptr<void>);
  void * getPtr(Uint32 i) const;
};
#endif

struct ArenaHead; // forward decl.
class ArenaAllocator; // forward decl.

template <typename P, typename T = typename P::Type>
class RecordPool {
public:
  typedef T Type;
  RecordPool();
  ~RecordPool();
  
  void init(Uint32 type_id, const Pool_context& pc);
  void wo_pool_init(Uint32 type_id, const Pool_context& pc);
  void arena_pool_init(ArenaAllocator*, Uint32 type_id, const Pool_context& pc);
  
  /**
   * Update p value for ptr according to i value 
   */
  void getPtr(Ptr<T> &) const;
  void getPtr(ConstPtr<T> &) const;
  
  /**
   * Get pointer for i value
   */
  T * getPtr(Uint32 i) const;
  const T * getConstPtr(Uint32 i) const;

  /**
   * Update p & i value for ptr according to <b>i</b> value 
   */
  [[nodiscard]] bool getPtr(Ptr<T> &, Uint32 i) const;
  void getPtr(ConstPtr<T> &, Uint32 i) const;

  /**
   * Allocate an object from pool - update Ptr
   *
   * Return i
   */
  bool seize(Ptr<T> &);

  /**
   * Allocate object from arena - update Ptr
   */
  bool seize(ArenaHead&, Ptr<T>&);

  /**
   * Return an object to pool
   */
  void release(Uint32 i);

  /**
   * Return an object to pool
   */
  void release(Ptr<T>);
private:
  P m_pool;
};

template <typename P, typename T>
inline
RecordPool<P, T>::RecordPool()
{
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::init(Uint32 type_id, const Pool_context& pc)
{
  T tmp;
  const char * off_base = (char*)&tmp;
  const char * off_next = (char*)&tmp.nextPool;
  const char * off_magic = (char*)&tmp.m_magic;

  Record_info ri;
  ri.m_size = sizeof(T);
  ri.m_offset_next_pool = Uint32(off_next - off_base);
  ri.m_offset_magic = Uint32(off_magic - off_base);
  ri.m_type_id = type_id;
  m_pool.init(ri, pc);
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::wo_pool_init(Uint32 type_id, const Pool_context& pc)
{
  T tmp;
  const char * off_base = (char*)&tmp;
  const char * off_magic = (char*)&tmp.m_magic;
  
  Record_info ri;
  ri.m_size = sizeof(T);
  ri.m_offset_next_pool = 0;
  ri.m_offset_magic = Uint32(off_magic - off_base);
  ri.m_type_id = type_id;
  m_pool.init(ri, pc);
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::arena_pool_init(ArenaAllocator* alloc,
                                  Uint32 type_id, const Pool_context& pc)
{
  T tmp;
  const char * off_base = (char*)&tmp;
  const char * off_next = (char*)&tmp.nextPool;
  const char * off_magic = (char*)&tmp.m_magic;

  Record_info ri;
  ri.m_size = sizeof(T);
  ri.m_offset_next_pool = Uint32(off_next - off_base);
  ri.m_offset_magic = Uint32(off_magic - off_base);
  ri.m_type_id = type_id;
  m_pool.init(alloc, ri, pc);
}


template <typename P, typename T>
inline
RecordPool<P, T>::~RecordPool()
{
}

  
template <typename P, typename T>
inline
void
RecordPool<P, T>::getPtr(Ptr<T> & ptr) const
{
  ptr.p = static_cast<T*>(m_pool.getPtr(ptr.i));
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::getPtr(ConstPtr<T> & ptr) const 
{
  ptr.p = static_cast<const T*>(m_pool.getPtr(ptr.i));
}

template <typename P, typename T>
inline
bool
RecordPool<P, T>::getPtr(Ptr<T> & ptr, Uint32 i) const
{
  if (unlikely(i >= RNIL))
  {
    assert(i == RNIL);
    return false;
  }
  ptr.i = i;
  ptr.p = static_cast<T*>(m_pool.getPtr(ptr.i));  
  return true;
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::getPtr(ConstPtr<T> & ptr, Uint32 i) const 
{
  ptr.i = i;
  ptr.p = static_cast<const T*>(m_pool.getPtr(ptr.i));  
}
  
template <typename P, typename T>
inline
T * 
RecordPool<P, T>::getPtr(Uint32 i) const
{
  return static_cast<T*>(m_pool.getPtr(i));  
}

template <typename P, typename T>
inline
const T * 
RecordPool<P, T>::getConstPtr(Uint32 i) const 
{
  return static_cast<const T*>(m_pool.getPtr(i)); 
}
  
template <typename P, typename T>
inline
bool
RecordPool<P, T>::seize(Ptr<T> & ptr)
{
  Ptr<T> tmp;
  bool ret = m_pool.seize(tmp);
  if(likely(ret))
  {
    ptr.i = tmp.i;
    ptr.p = static_cast<T*>(tmp.p);
  }
  return ret;
}

template <typename P, typename T>
inline
bool
RecordPool<P, T>::seize(ArenaHead & ah, Ptr<T> & ptr)
{
  Ptr<T> tmp;
  bool ret = m_pool.seize(ah, tmp);
  if(likely(ret))
  {
    ptr.i = tmp.i;
    ptr.p = static_cast<T*>(tmp.p);
  }
  return ret;
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::release(Uint32 i)
{
  Ptr<T> ptr;
  ptr.i = i;
  ptr.p = m_pool.getPtr(i);
  m_pool.release(ptr);
}

template <typename P, typename T>
inline
void
RecordPool<P, T>::release(Ptr<T> ptr)
{
  Ptr<T> tmp;
  tmp.i = ptr.i;
  tmp.p = ptr.p;
  m_pool.release(tmp);
}

template <typename P, typename T = typename P::Type>
class RecordPool64 {
public:
  typedef T Type;
  RecordPool64();
  ~RecordPool64();
  
  void init(Uint32 type_id, const Pool_context& pc);

  bool checkMagic(void *record) const;
  /**
   * Update p value for ptr according to i value 
   */
  void getUncheckedPtr(Ptr64<T> &) const;
  [[nodiscard]] bool getPtr(Ptr64<T> &) const;
  [[nodiscard]] bool getPtr(ConstPtr64<T> &) const;
  
  /**
   * Get pointer for i value
   */
  T * getPtr(Uint64 i) const;
  const T * getConstPtr(Uint64 i) const;

  /**
   * Update p & i value for ptr according to <b>i</b> value 
   */
  [[nodiscard]] bool getPtr(Ptr64<T> &, Uint64 i) const;
  [[nodiscard]] bool getPtr(ConstPtr64<T> &, Uint64 i) const;

  /**
   * Allocate an object from pool - update Ptr
   *
   * Return i
   */
  bool seize(Ptr64<T> &);

  /**
   * Return an object to pool
   */
  void release(Uint64 i);

  /**
   * Return an object to pool
   */
  void release(Ptr64<T>);
private:
  P m_pool;
};

template <typename P, typename T>
inline
RecordPool64<P, T>::RecordPool64()
{
}

template <typename P, typename T>
inline
void
RecordPool64<P, T>::init(Uint32 type_id, const Pool_context& pc)
{
  T tmp;
  const char * off_base = (char*)&tmp;
  const char * off_next = (char*)&tmp.nextPool;
  const char * off_magic = (char*)&tmp.m_magic;

  Record_info ri;
  ri.m_size = sizeof(T);
  ri.m_offset_next_pool = Uint32(off_next - off_base);
  ri.m_offset_magic = Uint32(off_magic - off_base);
  ri.m_type_id = type_id;
  m_pool.init(ri, pc);
}

template <typename P, typename T>
inline
RecordPool64<P, T>::~RecordPool64()
{
}

template <typename P, typename T>
inline
bool
RecordPool64<P, T>::checkMagic(void *record) const
{
  return m_pool.checkMagic(record);
}

template <typename P, typename T>
inline
void
RecordPool64<P, T>::getUncheckedPtr(Ptr64<T> & ptr) const
{
  ptr.p = static_cast<T*>(m_pool.getUncheckedPtr(ptr.i));
}

template <typename P, typename T>
inline
bool
RecordPool64<P, T>::getPtr(Ptr64<T> & ptr) const
{
  ptr.p = static_cast<T*>(m_pool.getPtr(ptr.i));
  return ptr.p != nullptr;
}

template <typename P, typename T>
inline
bool
RecordPool64<P, T>::getPtr(ConstPtr64<T> & ptr) const 
{
  ptr.p = static_cast<const T*>(m_pool.getPtr(ptr.i));
  return ptr.p != nullptr;
}

template <typename P, typename T>
inline
bool
RecordPool64<P, T>::getPtr(Ptr64<T> & ptr, Uint64 i) const
{
  ptr.i = i;
  ptr.p = static_cast<T*>(m_pool.getPtr(ptr.i));  
  return ptr.p != nullptr;
}

template <typename P, typename T>
inline
bool
RecordPool64<P, T>::getPtr(ConstPtr64<T> & ptr, Uint64 i) const 
{
  ptr.i = i;
  ptr.p = static_cast<const T*>(m_pool.getPtr(ptr.i));  
  return ptr.p != nullptr;
}
  
template <typename P, typename T>
inline
T * 
RecordPool64<P, T>::getPtr(Uint64 i) const
{
  return static_cast<T*>(m_pool.getPtr(i));  
}

template <typename P, typename T>
inline
const T * 
RecordPool64<P, T>::getConstPtr(Uint64 i) const 
{
  return static_cast<const T*>(m_pool.getPtr(i)); 
}
  
template <typename P, typename T>
inline
bool
RecordPool64<P, T>::seize(Ptr64<T> & ptr)
{
  Ptr64<T> tmp;
  bool ret = m_pool.seize(tmp);
  if(likely(ret))
  {
    ptr.i = tmp.i;
    ptr.p = static_cast<T*>(tmp.p);
  }
  return ret;
}

template <typename P, typename T>
inline
void
RecordPool64<P, T>::release(Uint64 i)
{
  Ptr64<T> ptr;
  ptr.i = i;
  ptr.p = m_pool.getPtr(i);
  m_pool.release(ptr);
}

template <typename P, typename T>
inline
void
RecordPool64<P, T>::release(Ptr64<T> ptr)
{
  Ptr64<T> tmp;
  tmp.i = ptr.i;
  tmp.p = ptr.p;
  m_pool.release(tmp);
}

#undef JAM_FILE_ID

#endif
