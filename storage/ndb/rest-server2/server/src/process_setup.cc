/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

#include "process_setup.hpp"

#include <cerrno>

#ifdef __linux__
#include <malloc.h>
#include <sys/prctl.h>
#endif

ProcessSetupResult rdrs_process_setup() {
  ProcessSetupResult result = {false, false, 0, 0};
#ifdef __linux__
  /* rdrs2 maps a large, sparse address space (per-thread arenas, 8 MiB
   * stacks, the buffer pool). On nodes with transparent_hugepage/enabled
   * set to always, khugepaged collapses those sparse regions into resident
   * 2 MiB pages until an idle pod reaches its memory limit. The flag is
   * per process and inherited across fork and exec, so it covers every
   * thread created after this point regardless of the node setting. */
  errno = 0;
  if (prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) == 0) {
    result.thp_disabled = true;
  } else {
    result.errno_thp = errno;
  }
  /* mallopt returns 1 on success, 0 on error (and does not set errno). */
  if (mallopt(M_ARENA_MAX, RDRS_MALLOC_ARENA_MAX) == 1) {
    result.arena_max_set = true;
  } else {
    result.errno_arena = EINVAL;
  }
#endif
  return result;
}
