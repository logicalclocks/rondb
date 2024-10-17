/*
   Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.

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

#ifndef STORAGE_NDB_SRC_RONSQL_ARENAMALLOC_HPP
#define STORAGE_NDB_SRC_RONSQL_ARENAMALLOC_HPP 1

#include <assert.h>
#include <stdexcept>
#include <cstddef>
#include "ndb_types.h"
#include "my_compiler.h"

typedef Uint8 byte;

//#define ARENAMALLOC_DEBUG 1

class ArenaMalloc
{
private:
  struct Page
  {
    struct Page* next = nullptr;
    byte data[1]; // Actually an arbitrary amount
  };
  static const size_t OVERHEAD = offsetof(struct Page, data);
  static const size_t MINIMUM_PAGE_SIZE = 64;
  static const size_t MAXIMUM_ALLOCATION_SIZE = SIZE_MAX >> 2;
  static const size_t MAXIMUM_ALIGNMENT = 16;
  friend UintPtr aligned(size_t align, UintPtr ptr) noexcept;
  static_assert(alignof(std::max_align_t) <= MAXIMUM_ALIGNMENT,
                "MAXIMUM_ALIGNMENT too small");
  static_assert(OVERHEAD < MINIMUM_PAGE_SIZE, "MINIMUM_PAGE_SIZE too small");
  static_assert((2 * (OVERHEAD + MAXIMUM_ALIGNMENT)) < MINIMUM_PAGE_SIZE,
                "MINIMUM_PAGE_SIZE too small");
  // We need page_size/2 more often than we need page_size.
  size_t m_half_page_size;
  struct Page* m_current_page;
  struct Page* m_large_allocations;
  UintPtr m_point;
  UintPtr m_stop;
#ifdef ARENAMALLOC_DEBUG
  Uint64 m_allocated_by_us;
  Uint64 m_allocated_by_user;
  Uint64 m_page_count;
  Uint64 m_large_alloc_count;
  Uint64 m_small_alloc_count;
#endif
  void free_memory() noexcept;
  void init_object(size_t) noexcept;
public:
  // No default constructor, always require specifying parameters.
  ArenaMalloc() = delete;
  ArenaMalloc(size_t page_size) noexcept;
  ~ArenaMalloc() noexcept;
  void reset() noexcept;
  void reset(size_t page_size) noexcept;
  void* alloc_bytes(size_t size, size_t alignment) noexcept;
  template<typename T> inline T* alloc(Uint32 items) noexcept;
  template<typename T> inline T* alloc_exc(Uint32 items);
  void* realloc_bytes(const void* ptr,
                      size_t size,
                      size_t original_size,
                      size_t alignment
                     ) noexcept;
  template<typename T> inline T* realloc(const T* ptr,
                                         Uint32 items,
                                         Uint32 original_items
                                        ) noexcept;
  template<typename T> inline T* realloc_exc(const T* ptr,
                                             Uint32 items,
                                             Uint32 original_items);
};

/*
 * Wrapper to allow allocation without type casts. Usage example:
 *   MyType* variable = arena_malloc->alloc<MyType>(5)
 * which is equivalent to:
 *  MyType* variable = (MyType*) arena_malloc->alloc_bytes(5 * sizeof(MyType),
 *                                                         alignof(MyType))
 * Return nullptr on failure.
 */
template <typename T>
inline T*
ArenaMalloc::alloc(Uint32 items) noexcept
{
  static_assert(alignof(T) <= MAXIMUM_ALIGNMENT);
  return static_cast<T*>(alloc_bytes(items * sizeof(T), alignof(T)));
}

/*
 * Like ArenaMalloc::alloc(), but throw an exception on failure.
 */
template <typename T>
inline T*
ArenaMalloc::alloc_exc(Uint32 items)
{
  T* ret = alloc<T>(items);
  if(likely(ret)) {
    return ret;
  }
  throw std::runtime_error("ArenaMalloc: Cannot allocate");
}

/*
 * Wrapper to allow reallocation without type casts. Usage example:
 *   MyType* variable = arena_malloc->realloc<MyType>(ptr, 5, 3)
 * which is equivalent to:
 *  MyType* variable = (MyType*) arena_malloc->realloc_bytes(ptr,
 *                                                           5 * sizeof(MyType),
 *                                                           3 * sizeof(MyType),
 *                                                           alignof(MyType))
 * Return nullptr on failure.
 *
 * WARNING: Read comment for ArenaMalloc::realloc_bytes().
 */
template <typename T>
inline T*
ArenaMalloc::realloc(const T* ptr, Uint32 items, Uint32 original_items) noexcept
{
  static_assert(alignof(T) <= MAXIMUM_ALIGNMENT);
  return static_cast<T*>
    (realloc_bytes
     (static_cast<const void*>(ptr),
      sizeof(T) * items,
      sizeof(T) * original_items,
      alignof(T)));
}

/*
 * Like ArenaMalloc::realloc(), but throw an exception on failure.
 */
template <typename T>
inline T*
ArenaMalloc::realloc_exc(const T* ptr, Uint32 items, Uint32 original_items)
{
  T* ret = realloc<T>(ptr, items, original_items);
  if(likely(ret)) {
    return ret;
  }
  throw std::runtime_error("ArenaMalloc: Cannot allocate");
}

/*
 * reset() will free all allocations and return the object to its initial state
 * without changing the page size.
 */
inline void
ArenaMalloc::reset() noexcept
{
  size_t page_size = m_half_page_size << 1;
  reset(page_size);
}

/*
 * reset(page_size) will free all allocations and return the object to its
 * initial state and change the page size.
 */
inline void
ArenaMalloc::reset(size_t page_size) noexcept {
  free_memory();
  init_object(page_size);
}

/*
 * Helper function. Return the lowest pointer `aligned` such that
 * `ptr <= aligned && aligned % align == 0`. This is similar in purpose to
 * std::align, but non-destructive. Note that `align` must be a power of two.
 */
inline UintPtr
aligned(size_t align, UintPtr ptr) noexcept
{
  assert(align <= ArenaMalloc::MAXIMUM_ALIGNMENT);
  // Alignment must be power of two
  assert((align & (align - 1)) == 0);
  return (ptr - 1u + align) & -align;
}
#endif
