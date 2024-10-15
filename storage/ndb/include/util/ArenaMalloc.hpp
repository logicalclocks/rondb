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

#ifndef STORAGE_NDB_SRC_RONSQL_ARENAALLOCATOR_HPP
#define STORAGE_NDB_SRC_RONSQL_ARENAALLOCATOR_HPP 1

#include <assert.h>
#include <stdexcept>
#include "ndb_types.h"

using std::byte;

//#define ARENA_ALLOCATOR_DEBUG 1

class ArenaAllocator
{
private:
  /*
   * todo: These two parameters could be dynamic. With some statistics, we
   * should be able to tune these as a function of SQL statement length, which
   * we'll probably know before we create the arena allocator.
   */
  static const size_t DEFAULT_PAGE_SIZE = 256;
  static const size_t INITIAL_PAGE_SIZE = 80;
  size_t m_page_size = DEFAULT_PAGE_SIZE;
  struct Page
  {
    struct Page* next = NULL;
    byte data[1]; // Actually an arbitrary amount
  };
  static const size_t OVERHEAD = offsetof(struct Page, data);
  static_assert(OVERHEAD < DEFAULT_PAGE_SIZE, "default page size too small");
  struct Page* m_current_page = NULL;
  UintPtr m_point = 0;
  UintPtr m_stop = 0;
# ifdef ARENA_ALLOCATOR_DEBUG
  Uint64 m_allocated_by_us = sizeof(ArenaAllocator);
  Uint64 m_allocated_by_user = 0;
# endif
  byte m_initial_stack_allocated_page[INITIAL_PAGE_SIZE];
public:
  ArenaAllocator();
  ~ArenaAllocator();
  void* alloc_bytes(size_t size, size_t alignment);
  template<typename T> inline T* alloc(Uint32 items);
  void* realloc_bytes(const void* ptr, size_t size, size_t original_size, size_t alignment);
  template<typename T> inline T* realloc(const T* ptr, Uint32 items, Uint32 original_items);
};

template <typename T>
inline T*
ArenaAllocator::alloc(Uint32 items)
{
  return static_cast<T*>(alloc_bytes(items * sizeof(T), alignof(T)));
}
template <typename T>
inline T*
ArenaAllocator::realloc(const T* ptr, Uint32 items, Uint32 original_items)
{
  return static_cast<T*>
    (realloc_bytes
     (static_cast<const void*>(ptr),
      sizeof(T) * items,
      sizeof(T) * original_items,
      alignof(T)));
}

#endif
