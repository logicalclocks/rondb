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

#include <cstring> // memcpy
#include <ArenaMalloc.hpp>
#ifdef ARENAMALLOC_DEBUG
#include <iostream>
#endif

/*
 * The page size also determines what allocations are considered large. Large
 * allocations are those that are at least half a page size, and they will be
 * allocated using individual calls to malloc(). Allocations smaller than that
 * will be allocated consecutively from a page.
 */
ArenaMalloc::ArenaMalloc(size_t page_size) noexcept {
  init_object(page_size);
}

ArenaMalloc::~ArenaMalloc() noexcept
{
  free_memory();
}

void ArenaMalloc::init_object(size_t page_size) noexcept {
  m_half_page_size = page_size >> 1;
  m_current_page = nullptr;
  m_large_allocations = nullptr;
  m_point = 0;
  m_stop = 0;
#ifdef ARENAMALLOC_DEBUG
  m_allocated_by_us = 0;
  m_allocated_by_user = 0;
  m_page_count = 0;
  m_large_alloc_count = 0;
  m_small_alloc_count = 0;
#endif
  // page_size must be sane
  assert(page_size >= MINIMUM_PAGE_SIZE);
  assert(page_size < (SIZE_MAX >> 3));
  assert(m_half_page_size < (page_size - OVERHEAD - MAXIMUM_ALIGNMENT));
  assert(page_size == (m_half_page_size << 1));
  // page_size must be a power of two
  assert((page_size & (page_size - 1)) == 0);
}

void
ArenaMalloc::free_memory() noexcept {
#ifdef ARENAMALLOC_DEBUG
  Uint32 page_count = 0;
  Uint32 large_count = 0;
  UintPtr last_page_data = reinterpret_cast<UintPtr>(&m_current_page->data[0]);
#endif
  while (m_current_page)
  {
    Page* next = (Page*)m_current_page->next;
    free(m_current_page);
    m_current_page = next;
#ifdef ARENAMALLOC_DEBUG
    page_count++;
#endif
  }
  while (m_large_allocations)
  {
    Page* next = m_large_allocations->next;
    free(m_large_allocations);
    m_large_allocations = next;
#ifdef ARENAMALLOC_DEBUG
    large_count++;
#endif
  }
#ifdef ARENAMALLOC_DEBUG
  assert(page_count == m_page_count);
  assert(large_count == m_large_alloc_count);
  if (m_page_count)
    std::cerr << "ArenaMalloc " << this << ": Page " << m_page_count << " used "
              << (m_point - last_page_data) << " / " << (m_half_page_size << 1)
              << std::endl;
  std::cerr << "ArenaMalloc" << this << ": reset()\n"
            << "  Pages: " << page_count << "\n"
            << "  Large allocations: " << large_count << "\n"
            << "  Small allocations: " << m_small_alloc_count << "\n"
            << "  Total allocated by us: " << m_allocated_by_us << "\n"
            << "  Total allocated by user: " << m_allocated_by_user << "\n"
            << "  Efficiency: " << 100 * m_allocated_by_user / m_allocated_by_us
            << "%" << std::endl;
#endif
  assert(m_current_page == nullptr);
  assert(m_large_allocations == nullptr);
}

/*
 * Allocate a certain number of bytes with a given alignment. Return nullptr if
 * allocation was unsuccessful.
 */
void*
ArenaMalloc::alloc_bytes(size_t size, size_t alignment) noexcept
{
  assert(alignment <= MAXIMUM_ALIGNMENT);
  // Large allocations
  if (unlikely(m_half_page_size <= size)) {
    if (unlikely(MAXIMUM_ALLOCATION_SIZE < size)) {
      return nullptr;
    }
    size_t new_allocation_size = size + OVERHEAD + alignment - 1;
    Page* new_allocation = static_cast<Page*>
      (malloc(new_allocation_size));
    if (likely(new_allocation)) {
#ifdef ARENAMALLOC_DEBUG
      m_allocated_by_us += new_allocation_size;
      m_allocated_by_user += size;
      std::cerr << "ArenaMalloc " << this
                << ": Large allocation " << size
                << " / " << new_allocation_size << std::endl;
#endif
      new_allocation->next = m_large_allocations;
      m_large_allocations = new_allocation;
      void* ret = reinterpret_cast<void*>
        (aligned(alignment,
                 reinterpret_cast<UintPtr>(&new_allocation->data[0])));
      assert((reinterpret_cast<UintPtr>(ret) + size) <=
             (reinterpret_cast<UintPtr>(new_allocation) + new_allocation_size));
      assert((reinterpret_cast<UintPtr>(new_allocation) + new_allocation_size) <
             (reinterpret_cast<UintPtr>(ret) + size + alignment));
#ifdef ARENAMALLOC_DEBUG
      m_large_alloc_count++;
#endif
      return ret;
    }
    return nullptr;
  }
  // Small allocations
#ifdef ARENAMALLOC_DEBUG
  UintPtr orig_point = m_point;
#endif
  m_point = aligned(alignment, m_point);
  UintPtr new_point = m_point + size;
  if (unlikely(m_stop < new_point))
  {
    size_t page_size = m_half_page_size << 1;
    Page* new_page = static_cast<Page*>(malloc(page_size));
    if (likely(new_page)) {
#ifdef ARENAMALLOC_DEBUG
      m_allocated_by_us += page_size;
      if (m_page_count)
        std::cerr << "ArenaMalloc " << this << ": Page " << m_page_count
                  << " used "
                  << (orig_point -
                      reinterpret_cast<UintPtr>(&m_current_page->data[0]))
                  << " / " << page_size << std::endl;
      m_page_count++;
#endif
      new_page->next = m_current_page;
      m_current_page = new_page;
      m_point = reinterpret_cast<UintPtr>(&new_page->data[0]);
      m_stop = reinterpret_cast<UintPtr>(new_page) + page_size;
      m_point = aligned(alignment, m_point);
      new_point = m_point + size;
      // Since a small allocation is less than half a page, a stronger assertion
      // is possible, but this is simpler.
      assert(new_point <= m_stop);
    }
    else {
      return nullptr;
    }
  }
  void* ret = reinterpret_cast<void*>(m_point);
  m_point = new_point;
#ifdef ARENAMALLOC_DEBUG
  m_allocated_by_user += size;
  m_small_alloc_count++;
#endif
  return ret;
}

/*
 * Reallocate a certain number of bytes with a given alignment. Return nullptr
 * if reallocation was unsuccessful. The original allocation can be any pointer
 * and does not have to be allocated by the same, or any, ArenaMalloc.
 *
 * WARNING: The original ptr argument will NOT be freed.
 *
 * WARNING: ArenaMalloc::realloc_bytes can return a non-const pointer to the
 *          same memory as the argument `const void* ptr`. Make sure not to
 *          write to return_value[X] for any X<original_size.
 *
 * This reallocation function differs from the standard by requiring the size of
 * the original allocation. This gives several advantages:
 * 1) ArenaMalloc has no need to keep track of allocation sizes.
 * 2) ArenaMalloc can reallocate memory that was not allocated by ArenaMalloc;
 *    even compile-time constants.
 * 3) ArenaMalloc can potentially reallocate in-place a block of memory that
 *    is the result of concatenation.
 */
void*
ArenaMalloc::realloc_bytes(const void* ptr,
                           size_t size,
                           size_t original_size,
                           size_t alignment
                          ) noexcept
{
  assert(alignment <= MAXIMUM_ALIGNMENT);
  const byte* byte_ptr = static_cast<const byte*>(ptr);
  if (unlikely(
        &byte_ptr[original_size] == reinterpret_cast<byte*>(m_point) &&
        &byte_ptr[size] <= reinterpret_cast<byte*>(m_stop) &&
        original_size <= size &&
        aligned(alignment, m_point) == m_point)) {
    // The original allocation ends precisely where our unused memory begins,
    // the unused memory on the current page is sufficient to accommodate the
    // new allocation, the reallocation will increase the size, and the original
    // allocation fulfills our new alignment requirement. Therefore, we can
    // reallocate in-place.
    m_point += (size - original_size);
    assert(reinterpret_cast<byte*>(m_point) == &byte_ptr[size]);
    void* nonconst_ptr = reinterpret_cast<void*>(m_point - size);
    assert((const void*)nonconst_ptr == ptr);
    // Since we have reallocated in-place, no memcpy is necessary.
    return nonconst_ptr;
  }
  // Do not reallocate in-place.
  void* new_alloc = alloc_bytes(size, alignment);
  if (likely(new_alloc)) {
    size_t cplen = size < original_size ? size : original_size;
    // Since new_alloc is always new memory, we know that new_alloc and ptr will
    // never overlap.
    memcpy(new_alloc, ptr, cplen);
    return new_alloc;
  }
  return nullptr;
}
