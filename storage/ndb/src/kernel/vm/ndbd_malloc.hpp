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

#ifndef NDBD_MALLOC_H
#define NDBD_MALLOC_H

#include <stddef.h>

#include "ndb_types.h"

#define JAM_FILE_ID 234

/**
 * common memory allocation function for ndbd kernel
 */
void *ndbd_malloc(size_t size);
bool ndbd_malloc_need_watchdog(size_t size);
void *ndbd_malloc_watched(size_t size, volatile Uint32* watch_dog);
void ndbd_free(void *p, size_t size);
void ndbd_alloc_touch_mem(void * p, size_t sz, volatile Uint32 * watchCounter, bool make_readwritable);

/**
 * These functions can be used directly by blocks and other entities
 * to manage memory in an efficient manner.
 *
 * init_lc_ndbd_memory_pool
 *    Parameters:
 *      num_pools:         [IN] The number of memory pools to allocate from
 *      num_pool_threads:  [IN] The number of internal pools per long lived
 *                              pool
 *      num_split_threads: [IN] The number of internal pools per short lived
 *                              pool
 *
 *   This call is used to initialise the memory to handle the various
 *   pools that use this interface. It is called at startup before
 *   any calls to allocate memory through its interfaces have been done.
 *
 *  General malloc/free functions for NDB kernel
 *  --------------------------------------------
 *
 * lc_ndbd_pool_malloc
 *   Parameters:
 *     size:       [IN] The size of the memory to allocate in bytes
 *     pool_id:    [IN] The memory pool to use in the allocations
 *     thread_id:  [IN] The thread id that calls, used to derive the
 *                      internal pool to use
 *     clear_flag: [IN] If set clears the memory (set to 0) before returning
 *
 *   Return value:
 *     == 0     Unsuccessful allocation of memory
 *     != 0     The address of the memory allocated
 *
 *   This call allocates a memory area of exactly the requested size.
 *   It carries a pool id that identifies the global memory pool to
 *   allocate the memory from. In practice the memory will always be
 *   extended to be a multiple of 16 bytes.
 *
 * lc_ndbd_pool_free
 *   Parameters:
 *     mem:        [IN] The start of the memory area to free
 *   Return value:
 *     None         The call will crash the data node if unsuccessful
 *
 *  This call returns the memory area to the pool where it was acquired
 *  from.
 *
 *  Specialized malloc/free functions for very short-lived memory segments
 *  ----------------------------------------------------------------------
 * lc_ndbd_split_malloc
 *   Parameters:
 *     mem:        [IN] The start of the memory area to split
 *     size:       [IN] The size of the memory area that should be kept at
 *                      the start address.
 *     pool_id:    [IN] The memory pool to use in the allocations
 *     thread_id:  [IN] The thread id that calls, used to derive the
 *                      internal pool to use
 *   Return value:
 *     == 0     Unsuccessful split of memory
 *     != 0     The address of the new memory split off
 *
 *   This call is used to allocate a memory region that will only be allocated
 *   for a very short time. This means that we will allocate consecutive
 *   memory and ignore bad effects from memory kept for a long time.
 *
 * lc_ndbd_split_free
 *   Parameters:
 *     mem:        [IN] The start of the split memory area to free
 *   Return value:
 *     None         The call will crash the data node if unsuccessful
 *
 *  This call returns the memory area to the pool where it was acquired
 *  from. But it waits for the entire area to be released before this
 *  can occur. Thus it is imperative that users of this interface do not
 *  hold onto memory for any longer periods.
 *
 * The above provides the details guaranteed by the API. The implementation
 * of those calls can vary dependent on which pool_id that is used.
 * There are very different use scenarios for different pools. E.g. the
 * SchemaMemory contains objects that are kept for a very long time and
 * that are only allocated and freed when a new metadata object is
 * created, altered or droppped. At the other extreme we have memory areas
 * used by signals to carry the signal payload. These are extremely short
 * lived and uses very frequent allocations and frees of fairly small
 * memory areas.
 */

/**
 * common memory allocation function for ndbd kernel
 *
 * There are two different sets of functions.
 * lc_ndbd_pool_malloc/free for generic malloc towards a pool
 *
 * lc_ndbd_split_malloc/free
 * to handle short lived memory allocations that are provided in a
 * consecutive memory space.
 */

/**
 * The lc_ndbd_pool_malloc/free is a traditional malloc/free API.
 * The malloc call carries a pool id and a thread id to ensure that
 * we know from which memory pool to request the memory. There is also
 * a clear flag to ensure that we don't need a specific calloc call.
 */
void *lc_ndbd_pool_malloc(size_t size,
                          Uint32 pool_id,
                          Uint32 thread_id,
                          bool clear_flag);
void lc_ndbd_pool_free(void *mem);

/**
 * The interface to
 * - lc_ndbd_split_malloc
 * - lc_ndbd_split_free
 * is a bit different.
 *
 * The user of this interface will start by declaring a void* variable
 * which is assigned nullptr. This variable is sent into lc_ndbd_split_malloc
 * by reference, thus lc_ndbd_split_malloc will update with a reference to
 * a memory area from where to allocate the next memory area. If the
 * method lc_ndbd_split_malloc finds that this memory area is exhausted it
 * will allocate a new area and update the referenced memory area pointer.
 *
 * The method lc_ndbd_split_malloc needs the pool_id variable to allocate new
 * memory areas when needed. The thread_id is required to use the correct
 * memory area instances when allocating new memory.
 *
 * The memory area returned by lc_ndbd_split_malloc can be returned by any
 * thread using lc_ndbd_split_free. However the interface requires those
 * memory allocations to be short-lived memory allocations since the entire
 * memory area is kept allocated until all parts have been returned.
 *
 * There is no call to return a memory area, it is expected that a thread
 * that uses this interface runs until the program is stopped and it is
 * up to higher levels in the software stack to ensure that any memory
 * allocated is returned to the OS.
 *
 * When all memory areas returned from lc_ndbd_split_malloc using one
 * void* pointer has called lc_ndbd_split_free, then it will be
 * discovered that the entire area is free'd and the area will be
 * available for a new lc_ndbd_split_malloc calls.
 *
 * It is vital that memory allocated in this interface is never held
 * onto for any extended periods of time. The normal use is to allocate
 * memory in one thread receiving messages, store the message in this
 * memory and send the message to its destination. The memory is free'd
 * as soon as the message have been handled.
 */
void *lc_ndbd_split_malloc(void **mem,
                           size_t size,
                           unsigned int pool_id,
                           unsigned int thread_id);
void lc_ndbd_split_free(void *mem);

/**
 * We will maintain a set of memory pools for each memory region, e.g.
 * Schema memory is one such region, Transaction Memory is another such
 * region. The purpose of the sets is to avoid mutex contention when
 * allocating memory.
 *
 * The ordinary malloc pools are used by all block threads and the number
 * of pools should be based on the number of block threads. The number
 * of split threads is mainly to ensure that the receive threads in
 * the data node can allocate without contention with other receive
 * threads.
 *
 * Some of those pools will not be used, this isn't a problem since
 * they will not allocate any substantial amount of memory, only a
 * few bytes for control structures. Thus we optimise on simple
 * mappings from memory region and thread id to pool.
 *
 * Each pool that is actually used will use at least 2 MByte of
 * memory which is the amount of memory we allocate from the
 * global memory pool each time we run out of memory in the pool.
 */
typedef void* (*LC_MALLOC_BACKEND) (size_t, unsigned int, unsigned int*);
typedef void (*LC_FREE_BACKEND) (void*, size_t, unsigned int, unsigned int);

void init_lc_ndbd_memory_pool(unsigned int num_pools,
                              unsigned int num_pool_threads,
                              unsigned int num_split_threads,
                              LC_MALLOC_BACKEND malloc_backend,
                              LC_FREE_BACKEND free_backend);

void stop_lc_ndbd_memory_pool();
#undef JAM_FILE_ID

#endif 
