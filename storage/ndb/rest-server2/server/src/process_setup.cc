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
#include <sys/prctl.h>
#endif

ProcessSetupResult rdrs_process_setup() {
  ProcessSetupResult result = {false, 0};
#ifdef __linux__
  /* rdrs2 maps a large, sparse address space (per-thread arenas, 8 MiB
   * stacks, the buffer pool). On nodes with transparent_hugepage/enabled
   * set to always, khugepaged collapses those sparse regions into resident
   * 2 MiB pages until an idle pod reaches its memory limit. The flag
   * belongs to the address space (all threads) and is inherited across
   * fork and exec. Before Linux 5.0 it only applied to mappings created
   * after the call, so it is set before the first thread maps its stack
   * and malloc arena. */
  errno = 0;
  if (prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) == 0) {
    result.thp_disabled = true;
  } else {
    result.errno_thp = errno;
  }
  /* No mallopt(M_ARENA_MAX): an earlier revision capped glibc at two
   * arenas to bound idle memory. Measured on Linux (64 CPUs, 83 server
   * threads) that cap serialised the NDB API's per-signal malloc and free
   * on two mutexes, cost up to 54 % of batch throughput, and retained
   * MORE memory after a blob burst (two fragmented heaps cannot be
   * trimmed). With THP disabled the per-thread arenas stay sparse, which
   * was the real idle cost. */
#endif
  return result;
}
