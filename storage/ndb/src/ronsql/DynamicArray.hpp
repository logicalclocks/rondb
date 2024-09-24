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

#ifndef STORAGE_NDB_SRC_RONSQL_DYNAMICARRAY_HPP
#define STORAGE_NDB_SRC_RONSQL_DYNAMICARRAY_HPP 1

#include "ArenaAllocator.hpp"

/* A simple dynamic array supporting only objects that are trivially
 * constructible/destructible, only arena allocation and no shrinking.
 *
 * The implementation holds an array of pointers to pages, each page holding
 * (1 << BITS) items. This allows for constant-time lookups using two pointer
 * dereferences. As the array expands, the array of pointers is reallocated as
 * necessary, doubling its capacity each time. This allows amortized constant-
 * time push and a memory overhead of roughly 1 byte per item. The items
 * themselves are never moved.
 */

template<typename T>
class DynamicArray
{
private:
  /*
   * todo: These two parameters could be dynamic. With some statistics, we
   * should be able to tune these as a function of SQL statement length, which
   * we'll know before we create any DynamicArray. Especially the BITS parameter
   * has a significant impact on memory use.
   */
  static const Uint32 INITIAL_PAGES_CAPACITY = 8;
  static const Uint32 BITS = 5;
  static const Uint32 ITEMS_PER_PAGE = (1 << BITS);
  static const Uint32 IDX_MASK = (1<<BITS)-1;
  T** pages;
  Uint32 item_count;
  Uint32 pages_capacity;
  ArenaAllocator* allocator;
public:
  DynamicArray(ArenaAllocator* alloc) :
    pages(NULL),
    item_count(0),
    pages_capacity(0),
    allocator(alloc)
  {
    assert(alloc != NULL);
  }
  ~DynamicArray()
  {
    // No deallocation needed
  }
  /*
   * Add an item to the end of the array
   */
  void push(const T& item)
  {
    if (!(item_count < (item_count + 1)))
    {
      // overflow_error inherits from runtime_error.
      throw std::overflow_error("DynamicArray::push: item count overflow");
    }
    Uint32 page = (item_count >> BITS);
    Uint32 idx = item_count & IDX_MASK;
    if (idx == 0)
    {
      if (page >= pages_capacity)
      {
        Uint32 newCapacity = (INITIAL_PAGES_CAPACITY) > (pages_capacity * 2)
                             ? (INITIAL_PAGES_CAPACITY)
                             : (pages_capacity * 2);
        T** newPages = allocator->alloc<T*>(newCapacity);
        if (pages_capacity > 0)
        {
          memcpy(newPages, pages, pages_capacity * sizeof(T*));
        }
        pages = newPages;
        pages_capacity = newCapacity;
      }
      pages[page] = allocator->alloc<T>(ITEMS_PER_PAGE);
    }
    pages[page][idx] = item;
    item_count++;
  }
  /*
   * Item access by index
   */
  T& operator[](Uint32 index)
  {
    assert(index < item_count);
    return pages[index >> BITS][index & IDX_MASK];
  }
  const T& operator[](Uint32 index) const
  {
    assert(0 <= index && index < item_count);
    return pages[index >> BITS][index & IDX_MASK];
  }
  /*
   * Return the number of items in the array.
   */
  Uint32 size() const
  {
    return item_count;
  }
  /*
   * Given a pointer to an item, determine whether it points to an item in this
   * array. This executes in linear time.
   */
  bool has_item(T* item)
  {
    Uint32 lastPage = (item_count-1) >> BITS;
    for (Uint32 page = 0; page <= lastPage; page++)
    {
      if (pages[page] <= item && item < pages[page] + ITEMS_PER_PAGE)
      {
        Uint32 idx = item - pages[page];
        T* wouldBe = &pages[page][idx];
        return ((page << BITS) | idx) < item_count && wouldBe == item;
      }
    }
    return false;
  }
  /*
   * Convenience function to return the last item in the array.
   */
  T& last_item()
  {
    Uint32 index = item_count-1;
    return pages[index >> BITS][index & IDX_MASK];
  }
  /*
   * Truncate the array. No data or metadata will be overwritten at subsequent
   * pushes, so a copy of the array object before truncation will be intact,
   * independent and usable.
   */
  void truncate()
  {
    item_count = 0;
    pages_capacity = 0;
    pages = NULL;
  }
};

#endif
