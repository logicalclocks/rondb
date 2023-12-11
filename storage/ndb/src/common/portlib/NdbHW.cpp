/*
   Copyright (c) 2013, 2023, Oracle and/or its affiliates.
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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "my_config.h"
#include "ndb_config.h"
#include "util/require.h"
#include <NdbHW.hpp>
#include <UtilBuffer.hpp>
#include <File.hpp>
#include <NdbTick.h>
#include <NdbThread.h>
#include <ndb_limits.h>
#include <stdint.h>
#include <stdio.h>
#include "../src/common/util/parse_mask.hpp"
#include <iostream>
#include <thread>

#define DEBUG_HW(arglist) do { fprintf arglist ; } while (0)
//#define DEBUG_HW(arglist) do { } while(0)

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

static int inited = 0;
static int initres = 0;

static Uint32 ncpu = 0;
static Uint64 ticks_per_us = 0;
static Uint32 avx2_supported = 0;

static struct ndb_hwinfo *g_ndb_hwinfo = nullptr;

static Uint32 *g_first_l3_cache = nullptr;
static Uint32 *g_first_virt_l3_cache = nullptr;
static Uint32 *g_num_l3_cpus = nullptr;
static Uint32 *g_num_l3_cpus_online = nullptr;
static Uint32 *g_num_virt_l3_cpus = nullptr;
static Uint32 *g_create_cpu_map = nullptr;

/**
 * initialize list of cpu
 */
static int init_hwinfo(struct ndb_hwinfo *);
static int init_cpudata(struct ndb_hwinfo *);
static struct ndb_hwinfo *Ndb_SetHWInfo();
static void Ndb_FreeHWInfo();
static int Ndb_ReloadCPUData(struct ndb_hwinfo *);
static int Ndb_ReloadHWInfo(struct ndb_hwinfo *);
static int NdbHW_Init_platform();
static void NdbHW_End_platform();

Uint32 is_avx2_supported()
{
  return avx2_supported;
}

int NdbHW_Init()
{
  /**
   * This function is called at startup of process.
   */
  if (inited)
  {
    return initres;
  }

  inited = 1;
  initres = -1;

  g_ndb_hwinfo = nullptr;
#ifdef _SC_NPROCESSORS_CONF
  {
    long tmp = sysconf(_SC_NPROCESSORS_CONF);
    if (tmp < 0)
    {
      perror("sysconf(_SC_NPROCESSORS_CONF) returned error");
      abort();
    }
    else
    {
      ncpu = (Uint32) tmp;
      DEBUG_HW((stderr,
                "%u CPUs found using sysconf(_SC_NPROCESSORS_CONF)\n",
                ncpu));
    }
  }
#elif _WIN32
  ncpu = GetActiveProcessorCount(ALL_PROCESSOR_GROUPS);
  DEBUG_HW((stderr,
            "%u CPUs found using GetActiveProcessorCount(ALL_PROCESSOR_GROUPS)\n",
            ncpu));
#else
  ncpu = std::thread::hardware_concurrency();
  DEBUG_HW((stderr,
            "%u CPUs found using std::thread::hardware_concurrency()\n",
            ncpu));
#endif
  if (ncpu == 0)
  {
    perror("ncpu == 0");
    abort();
  }

  ticks_per_us = 0;
#if defined (HAVE_LINUX_SCHEDULING)
#ifdef _SC_CLK_TCK
  long sct = sysconf(_SC_CLK_TCK);
  if (sct <= 0)
  {
    perror("sysconf(_SC_CLK_TCK) failed!");
    abort();
  }
  else
  {
    ticks_per_us = Uint64(1000000) / Uint64(sct);
  }
#endif
  if (ticks_per_us == 0)
  {
    perror("ticks_per_us == 0");
    abort();
  }
#endif
  if (NdbHW_Init_platform() != 0)
  {
    perror("Failed NdbHW_Init_platform()");
    abort();
  }
  if ((Ndb_SetHWInfo()) == nullptr)
  {
    return -1;
  }
  initres = 0;
  return 0;
}

void NdbHW_End()
{
  /**
   * This function is called at shutdown of process.
   */
  if (inited)
  {
    NdbHW_End_platform();
    ncpu = 0;
    inited = 0;
    initres = 0;
    Ndb_FreeHWInfo();
    free(g_first_l3_cache);
    free(g_first_virt_l3_cache);
    free(g_num_l3_cpus);
    free(g_num_l3_cpus_online);
    free(g_num_virt_l3_cpus);
    g_first_l3_cache = nullptr;
    g_first_virt_l3_cache = nullptr;
    g_num_l3_cpus = nullptr;
    g_num_virt_l3_cpus = nullptr;
    g_num_l3_cpus_online = nullptr;
  }
}

void
Ndb_InitRRGroups(Uint32 *rr_group,
                 Uint32 num_rr_groups,
                 Uint32 num_query_instances,
                 Uint32 max_threads)
{
  require(num_query_instances <= max_threads);
  require(num_rr_groups > 0);
  for (Uint32 i = 0; i < max_threads; i++)
  {
    rr_group[i] = 0xFFFFFFFF; //Ensure group not valid as init value
  }
  if (g_create_cpu_map)
  {
    memcpy(rr_group,
           g_create_cpu_map,
           sizeof(Uint32) * num_query_instances);
    for (Uint32 i = 0; i < num_query_instances; i++)
    {
      require(rr_group[i] < num_rr_groups);
    }
  }
  else
  {
    Uint32 rr_group_id = 0;
    for (Uint32 i = 0; i < num_query_instances; i++)
    {
      rr_group[i] = rr_group_id;
      rr_group_id++;
      if (rr_group_id == num_rr_groups)
      {
        rr_group_id = 0;
      }
    }
  }
}

void
Ndb_GetCoreCPUIds(Uint32 cpu_id, Uint32 *cpu_ids, Uint32 &num_cpus)
{
  struct ndb_hwinfo *hwinfo = g_ndb_hwinfo;
  require(hwinfo->is_cpuinfo_available);
  require(hwinfo->cpu_info[cpu_id].online);
  if (cpu_id >= hwinfo->cpu_cnt_max)
  {
    perror("CPU out of bounds in Ndb_GetCoreCPUIds");
    abort();
  }
  if (hwinfo->cpu_cnt == 1)
  {
    cpu_ids[0] = cpu_id;
    num_cpus = 1;
    return;
  }
  Uint32 index = 1;
  Uint32 core_id = hwinfo->cpu_info[cpu_id].core_id;
  Uint32 package_id = hwinfo->cpu_info[cpu_id].package_id;
  cpu_ids[0] = cpu_id;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    if ((hwinfo->cpu_info[i].online == 1) &&
        (hwinfo->cpu_info[i].core_id == core_id) &&
        (hwinfo->cpu_info[i].package_id == package_id) &&
        (i != cpu_id))
    {
      cpu_ids[index] = i;
      index++;
    }
  }
  num_cpus = index;
  return;
}

Uint32
Ndb_GetRRGroups(Uint32 ldm_threads)
{
  return (ldm_threads + MAX_RR_GROUP_SIZE) / MAX_RR_GROUP_SIZE;
}

Uint32
Ndb_GetFirstCPUInMap()
{
  return g_ndb_hwinfo->first_cpu_map;
}

Uint32
Ndb_GetNextCPUInMap(Uint32 cpu_id)
{
  require(cpu_id < g_ndb_hwinfo->cpu_cnt_max);
  return g_ndb_hwinfo->cpu_info[cpu_id].next_cpu_map;
}

static void
create_prev_list(struct ndb_hwinfo *hwinfo)
{
  Uint32 loop_count = 0;
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    Uint32 prev_cpu = RNIL;
    Uint32 next_cpu = g_first_virt_l3_cache[i];
    loop_count++;
    while (next_cpu != RNIL)
    {
      loop_count++;
      hwinfo->cpu_info[next_cpu].prev_virt_l3_cpu_map = prev_cpu;
      prev_cpu = next_cpu;
      next_cpu = hwinfo->cpu_info[next_cpu].next_virt_l3_cpu_map;
      require(loop_count < 10000);
    }
    require(loop_count < 10000);
  }
}

static void
create_cpu_list(struct ndb_hwinfo *hwinfo,
                Uint32 num_rr_groups,
                Uint32 num_query_instances)
{
  Uint32 prev_cpu = RNIL;
  Uint32 next_cpu = RNIL;
  Uint32 all_groups = hwinfo->num_virt_l3_caches;
  bool found = false;
  Uint32 found_query = 0;
  Uint32 first_virt_l3_cache[MAX_NUM_CPUS];
  for (Uint32 i = 0; i < all_groups; i++)
  {
    first_virt_l3_cache[i] = g_first_virt_l3_cache[i];
  }
  /**
   * Create a CPU list with 2 CPUs at a time from each virtual
   * L3 cache. This means that we can assign one LDM and one
   * other block thread that contains a Query instance.
   *
   * This code is highly dependant of the code in do_parse
   * in thread_config.cpp for Automatic Thread Config where
   * we make use of all CPUs available.
   */
  Uint32 num_groups = num_rr_groups;
  Uint32 inx = 0;
  do
  {
    found = false;
    for (Uint32 i = 0; i < num_groups; i++)
    {
      for (Uint32 j = 0; j < 2; j++)
      {
        next_cpu = first_virt_l3_cache[i];
        if (next_cpu == RNIL)
        {
          break;
        }
        found = true;
        if (prev_cpu != RNIL)
        {
          hwinfo->cpu_info[prev_cpu].next_cpu_map = next_cpu;
        }
        else
        {
          hwinfo->first_cpu_map = next_cpu;
        }
        prev_cpu = next_cpu;
        first_virt_l3_cache[i] =
          hwinfo->cpu_info[next_cpu].next_virt_l3_cpu_map;
        hwinfo->cpu_info[next_cpu].next_cpu_map = RNIL;
        g_create_cpu_map[inx++] = i;
        found_query++;
      }
    }
    if (!found && num_groups < all_groups)
    {
      num_groups = all_groups;
      found = true;
    }
  } while (found);
  require(found_query >= num_query_instances);
  return;
}

static Uint32
find_largest_virt_l3_group(struct ndb_hwinfo *hwinfo, Uint32 first_group)
{
  Uint32 max_id = RNIL;
  Uint32 max_size = 0;
  for (Uint32 i = first_group; i < hwinfo->num_virt_l3_caches; i++)
  {
    if (g_num_virt_l3_cpus[i] > max_size)
    {
      max_id = i;
      max_size = g_num_virt_l3_cpus[i];
    }
  }
  return max_id;
}

static void
swap_virt_l3_caches(Uint32 largest_id, Uint32 curr_pos)
{
  Uint32 largest_size = g_num_virt_l3_cpus[largest_id];
  Uint32 largest_first = g_first_virt_l3_cache[largest_id];
  Uint32 curr_size = g_num_virt_l3_cpus[curr_pos];
  Uint32 curr_first = g_first_virt_l3_cache[curr_pos];
  g_first_virt_l3_cache[largest_id] = curr_first;
  g_first_virt_l3_cache[curr_pos] = largest_first;
  g_num_virt_l3_cpus[largest_id] = curr_size;
  g_num_virt_l3_cpus[curr_pos] = largest_size;
}

static void
sort_virt_l3_caches(struct ndb_hwinfo *hwinfo)
{
  if (hwinfo->num_virt_l3_caches > 1)
  {
    for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches - 1; i++)
    {
      Uint32 largest_id = find_largest_virt_l3_group(hwinfo, i);
      if (largest_id != i && largest_id != RNIL)
      {
        swap_virt_l3_caches(largest_id, i);
      }
    }
  }
}

static void create_init_virt_l3_cache_list(struct ndb_hwinfo *hwinfo)
{
  Uint32 num_l3_caches = hwinfo->num_shared_l3_caches;
  Uint32 virt_l3_cache_index = 0;
  for (Uint32 i = 0; i < num_l3_caches; i++)
  {
    bool found = false;
    Uint32 num_cpus = g_num_l3_cpus[i];
    Uint32 next_cpu = g_first_l3_cache[i];
    Uint32 prev_cpu = RNIL;
    Uint32 count = 0;
    for (Uint32 j = 0; j < num_cpus; j++)
    {
      if (hwinfo->cpu_info[next_cpu].online)
      {
        count++;
        if (found)
        {
          hwinfo->cpu_info[prev_cpu].next_virt_l3_cpu_map = next_cpu;
        }
        else
        {
          found = true;
          g_first_virt_l3_cache[virt_l3_cache_index] = next_cpu;
        }
        hwinfo->cpu_info[next_cpu].next_virt_l3_cpu_map = RNIL;
        prev_cpu = next_cpu;
      }
      next_cpu = hwinfo->cpu_info[next_cpu].next_l3_cpu_map;
    }
    require(next_cpu == RNIL);
    if (count > 0)
    {
      g_num_virt_l3_cpus[virt_l3_cache_index] = count;
      virt_l3_cache_index++;
    }
  }
  /**
   * Sort CPUs such that we first list the efficiency cores to be used
   * by LDM threads as much as possible. We keep the original order of
   * the lists except that those with Power efficiency will be sorted
   * at the end of each L3 cache list.
   */
  for (Uint32 i = 0; i < virt_l3_cache_index; i++)
  {
    Uint32 new_core_first = RNIL;
    Uint32 new_core_last = RNIL;
    Uint32 new_power_first = RNIL;
    Uint32 new_power_last = RNIL;
    Uint32 l3_list = g_first_virt_l3_cache[i];
    do
    {
      Uint32 cpu_capacity = hwinfo->cpu_info[l3_list].cpu_capacity;
      Uint32 next_list = hwinfo->cpu_info[l3_list].next_virt_l3_cpu_map;
      if (cpu_capacity == 100)
      {
        if (new_core_first == RNIL)
        {
          new_core_first = l3_list;
          hwinfo->cpu_info[l3_list].prev_virt_l3_cpu_map = RNIL;
        }
        else
        {
          hwinfo->cpu_info[new_core_last].next_virt_l3_cpu_map = l3_list;
          hwinfo->cpu_info[l3_list].prev_virt_l3_cpu_map = new_core_last;
        }
        hwinfo->cpu_info[l3_list].next_virt_l3_cpu_map = RNIL;
        new_core_last = l3_list;
      }
      else
      {
        if (new_power_first == RNIL)
        {
          new_power_first = l3_list;
          hwinfo->cpu_info[l3_list].prev_virt_l3_cpu_map = RNIL;
        }
        else
        {
          hwinfo->cpu_info[new_power_last].next_virt_l3_cpu_map = l3_list;
          hwinfo->cpu_info[l3_list].prev_virt_l3_cpu_map = new_power_last;
        }
        hwinfo->cpu_info[l3_list].next_virt_l3_cpu_map = RNIL;
        new_power_last = l3_list;
      }
      l3_list = next_list;
    } while (l3_list != RNIL);
    if (new_core_first != RNIL)
    {
      g_first_virt_l3_cache[i] = new_core_first;
      hwinfo->cpu_info[new_core_last].next_virt_l3_cpu_map = new_power_first;
      if (new_power_first != RNIL)
      {
        hwinfo->cpu_info[new_power_first].prev_virt_l3_cpu_map = new_core_last;
      }
    }
    else
    {
      g_first_virt_l3_cache[i] = new_power_first;
    }
  }
  hwinfo->num_virt_l3_caches = virt_l3_cache_index;
}


/* Create lists of CPUs connected to a certain L3 cache */
static int
create_l3_cache_list(struct ndb_hwinfo *hwinfo)
{
  g_first_l3_cache = (Uint32*)
    malloc(sizeof(Uint32) * hwinfo->num_shared_l3_caches);
  g_num_l3_cpus = (Uint32*)
    malloc(sizeof(Uint32) * hwinfo->num_shared_l3_caches);
  g_num_l3_cpus_online = (Uint32*)
    malloc(sizeof(Uint32) * hwinfo->num_shared_l3_caches);

  Uint32 cpu_cnt_max = hwinfo->cpu_cnt_max;
  Uint32 max_virt_l3_groups = cpu_cnt_max;
  g_first_virt_l3_cache = (Uint32*)
    malloc(sizeof(Uint32) * max_virt_l3_groups);
  g_num_virt_l3_cpus = (Uint32*)
    malloc(sizeof(Uint32) * max_virt_l3_groups);

  if (g_first_virt_l3_cache == nullptr ||
      g_num_virt_l3_cpus == nullptr ||
      g_first_l3_cache == nullptr ||
      g_num_l3_cpus == nullptr ||
      g_num_l3_cpus_online == nullptr)

  {
    perror("malloc failed");
    abort();
    return -1;
  }
  if (hwinfo->num_shared_l3_caches == 0)
  {
    perror("No L3 cache group");
    abort();
    return -1;
  }
  for (Uint32 i = 0; i < ncpu; i++)
  {
    hwinfo->cpu_info[i].next_l3_cpu_map = RNIL;
    hwinfo->cpu_info[i].in_l3_cache_list = false;
  }
  for (Uint32 l3_cache_id = 0;
       l3_cache_id < hwinfo->num_shared_l3_caches;
       l3_cache_id++)
  {
    g_first_l3_cache[l3_cache_id] = RNIL;
    Uint32 prev_cpu_id = RNIL;
    Uint32 found = 0;
    Uint32 found_online = 0;
    for (Uint32 cpu_id = 0; cpu_id < ncpu; cpu_id++)
    {
      if (hwinfo->cpu_info[cpu_id].l3_cache_id == l3_cache_id &&
          hwinfo->cpu_info[cpu_id].in_l3_cache_list == false)
      {
        if (found == 0)
        {
          g_first_l3_cache[l3_cache_id] = cpu_id;
          prev_cpu_id = cpu_id;
        }
        else
        {
          require(prev_cpu_id != RNIL);
          hwinfo->cpu_info[prev_cpu_id].next_l3_cpu_map = cpu_id;
          prev_cpu_id = cpu_id;
        }
        hwinfo->cpu_info[cpu_id].in_l3_cache_list = true;
        found++;
        if (hwinfo->cpu_info[cpu_id].online)
        {
          found_online++;
        }
        Uint32 core_id = hwinfo->cpu_info[cpu_id].core_id;
        for (Uint32 i = cpu_id + 1; i < ncpu; i++)
        {
          if (hwinfo->cpu_info[i].core_id == core_id &&
              hwinfo->cpu_info[i].l3_cache_id == l3_cache_id)
          {
            require(prev_cpu_id != RNIL);
            require(hwinfo->cpu_info[i].in_l3_cache_list == false);
            hwinfo->cpu_info[prev_cpu_id].next_l3_cpu_map = i;
            hwinfo->cpu_info[i].in_l3_cache_list = true;
            prev_cpu_id = i;
            found++;
            if (hwinfo->cpu_info[i].online)
            {
              found_online++;
            }
          }
        }
      }
    }
    g_num_l3_cpus[l3_cache_id] = found;
    g_num_l3_cpus_online[l3_cache_id] = found_online;
    DEBUG_HW((stderr,
              "%u CPUs found and %u CPUs online in L3 cache group %u\n",
              found,
              found_online,
              l3_cache_id));
  }
  return 0;
}

static bool
check_if_virt_l3_cache_is_ok(struct ndb_hwinfo *hwinfo,
                             Uint32 group_size,
                             Uint32 num_groups,
                             Uint32 num_query_instances,
                             Uint32 min_group_size,
                             bool will_be_ok)
{
  Uint32 count_non_fit_groups = 0;
  Uint32 count_non_fit_group_cpus = 0;
  Uint32 count_extra_cpus = 0;
  Uint32 count_full_groups_found = 0;
  Uint32 count_non_full_groups_found = 0;
  Uint32 full_group_size = group_size;
  Uint32 non_full_group_size = group_size - 2;
  if (non_full_group_size < min_group_size)
    non_full_group_size = min_group_size;

  DEBUG_HW((stderr,
            "full group size: %u, non full group size: %u\n",
            full_group_size,
            non_full_group_size));
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    Uint32 num_cpus_in_group = g_num_virt_l3_cpus[i];
    DEBUG_HW((stderr,
              "num_cpus %u in group %u\n",
              num_cpus_in_group, i));
    bool found_full_group = false;
    while (num_cpus_in_group >= full_group_size && will_be_ok)
    {
      found_full_group = true;
      num_cpus_in_group -= full_group_size;
      count_full_groups_found++;
    }
    if (num_cpus_in_group >= non_full_group_size)
    {
      count_non_full_groups_found++;
    }
    else if (!found_full_group)
    {
      count_non_fit_groups++;
      count_non_fit_group_cpus += num_cpus_in_group;
    }
    else
    {
      count_extra_cpus += num_cpus_in_group;
      continue;
    }
    if (num_cpus_in_group > non_full_group_size)
    {
      count_extra_cpus += (num_cpus_in_group - min_group_size);
    }
  }
  DEBUG_HW((stderr,
            "Full groups: %u, Non-full groups: %u\n",
            count_full_groups_found,
            count_non_full_groups_found));
  /* Only count non full groups up until the searched number of groups */
  count_non_full_groups_found = MIN(count_non_full_groups_found,
                                    (num_groups - count_full_groups_found));
  Uint32 tot_query_found =
    count_full_groups_found * group_size +
    count_non_full_groups_found * non_full_group_size;

  if (count_extra_cpus == 0 && count_non_fit_groups == 1)
    tot_query_found += count_non_fit_group_cpus;

  DEBUG_HW((stderr,
            "Total Query instances found: %u\n", tot_query_found));
  return (tot_query_found >= num_query_instances);
}

static void
merge_virt_l3_cache_list(struct ndb_hwinfo *hwinfo,
                         Uint32 largest_list,
                         Uint32 second_largest_list,
                         Uint32 min_group_size)
{
  DEBUG_HW((stderr,
            "merge_virt_l3_cache_list, "
            " into group %u from group %u, "
            " min_group_size: %u\n",
            largest_list,
            second_largest_list,
            min_group_size));
  /**
   * Merge first list at end of second list. Make second list as long
   * as the minimum group size, but not larger than that to avoid
   * having to later split the group once more. Place the entries from
   * the first list at the end of the second list.
   *
   * Move last entry into removed entry if not removed entry was the
   * last entry.
   */
  Uint32 group_size = min_group_size;
  Uint32 num_cpus_in_first_list = g_num_virt_l3_cpus[largest_list];
  Uint32 first_cpu_next = g_first_virt_l3_cache[largest_list];
  Uint32 last_cpu_first;
  do
  {
    last_cpu_first = first_cpu_next;
    first_cpu_next = hwinfo->cpu_info[first_cpu_next].next_virt_l3_cpu_map;
  } while (first_cpu_next != RNIL);
  Uint32 first_in_sec_cpu_list = g_first_virt_l3_cache[second_largest_list];
  Uint32 max_moved_cpus = group_size - num_cpus_in_first_list;
  Uint32 moved_cpus = 0;
  DEBUG_HW((stderr,
            "max_moved_cpus: %u\n", max_moved_cpus));
  for (Uint32 i = 0; i < max_moved_cpus; i++)
  {
    if (first_in_sec_cpu_list != RNIL)
    {
      moved_cpus++;
      hwinfo->cpu_info[last_cpu_first].next_virt_l3_cpu_map =
        first_in_sec_cpu_list;
      last_cpu_first = first_in_sec_cpu_list;
      first_in_sec_cpu_list =
        hwinfo->cpu_info[first_in_sec_cpu_list].next_virt_l3_cpu_map;
      hwinfo->cpu_info[last_cpu_first].next_virt_l3_cpu_map = RNIL;
    }
    else
    {
      break;
    }
  }
  g_first_virt_l3_cache[second_largest_list] = first_in_sec_cpu_list;
  g_num_virt_l3_cpus[largest_list] += moved_cpus;
  g_num_virt_l3_cpus[second_largest_list] -= moved_cpus;

  require(((g_num_virt_l3_cpus[second_largest_list] == 0) &&
           (first_in_sec_cpu_list == RNIL)) ||
          ((g_num_virt_l3_cpus[second_largest_list] != 0) &&
           (first_in_sec_cpu_list != RNIL)));

  Uint32 num_virt_l3_caches = hwinfo->num_virt_l3_caches - 1;
  if (first_in_sec_cpu_list == RNIL &&
      g_first_virt_l3_cache[num_virt_l3_caches] != RNIL)
  {
    /* We move last item which isn't empty into the removed slot */
    g_first_virt_l3_cache[second_largest_list] =
      g_first_virt_l3_cache[num_virt_l3_caches];
    g_num_virt_l3_cpus[second_largest_list] = g_num_virt_l3_cpus[num_virt_l3_caches];
    hwinfo->num_virt_l3_caches = num_virt_l3_caches;
  }
  else if (first_in_sec_cpu_list == RNIL)
  {
    hwinfo->num_virt_l3_caches = num_virt_l3_caches;
  }
}

static void
split_group(struct ndb_hwinfo *hwinfo,
            Uint32 split_group_id,
            Uint32 check_group_size)
{
  /**
   * Removed group_size CPUs from the chosen group (which is the largest
   * group still existing). Place the removed group at the last position
   * in the array of L3 cache groups. The original is kept in its original
   * position with the first group_size CPUs removed to the new list.
   * Increment number of virtual L3 caches.
   */
  DEBUG_HW((stderr,
            "Split L3 group %u, current size: %u, group_size: %u\n",
            split_group_id,
            g_num_virt_l3_cpus[split_group_id],
            check_group_size));
  g_num_virt_l3_cpus[hwinfo->num_virt_l3_caches] =
    g_num_virt_l3_cpus[split_group_id] - check_group_size;
  g_num_virt_l3_cpus[split_group_id] = check_group_size;
  Uint32 next_cpu = g_first_virt_l3_cache[split_group_id];
  Uint32 prev_cpu = RNIL;
  for (Uint32 i = 0; i < check_group_size; i++)
  {
    prev_cpu = next_cpu;
    next_cpu = hwinfo->cpu_info[next_cpu].next_virt_l3_cpu_map;
  }
  require(next_cpu != RNIL);
  require(prev_cpu != RNIL);
  Uint32 last_group_id = hwinfo->num_virt_l3_caches;
  g_first_virt_l3_cache[last_group_id] = next_cpu;
  hwinfo->cpu_info[prev_cpu].next_virt_l3_cpu_map = RNIL;
  hwinfo->num_virt_l3_caches++;
}

static void
adjust_rr_group_sizes(struct ndb_hwinfo *hwinfo,
                      Uint32 num_rr_groups,
                      Uint32 num_query_instances)
{
  if(num_rr_groups == 0)
    return;
  Uint32 group_size =
    (num_query_instances + (num_rr_groups - 1)) / num_rr_groups;
  Uint32 non_full_groups =
    (group_size * num_rr_groups) - num_query_instances;
  Uint32 full_groups = num_rr_groups - non_full_groups;
  require(full_groups > 0);
  for (Uint32 i = 0; i < num_rr_groups; i++)
  {
    Uint32 check_group_size = group_size;
    if (i >= full_groups)
    {
      check_group_size = (group_size - 1);
    }
    if (num_rr_groups > 1 && g_num_virt_l3_cpus[i] > check_group_size)
    {
      split_group(hwinfo,
                  i,
                  check_group_size);
    }
  }
}

static bool
split_virt_l3_cache_list(struct ndb_hwinfo *hwinfo,
                         Uint32 group_size)
{
  DEBUG_HW((stderr,
            "split_virt_l3_cache_list\n"));
  Uint32 check_group_size = group_size;
  Uint32 largest_group_size = 0;
  Uint32 largest_group_id = RNIL;
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    if (g_num_virt_l3_cpus[i] > largest_group_size)
    {
      largest_group_id = i;
      largest_group_size = g_num_virt_l3_cpus[i];
    }
  }
  if (largest_group_size <= check_group_size)
  {
    return false;
  }
  DEBUG_HW((stderr,
            "Split Group[%u] = %u\n",
            largest_group_id,
            largest_group_size));
  split_group(hwinfo,
              largest_group_id,
              check_group_size);
  return true;
}

static bool
create_min_virt_l3_cache_list(struct ndb_hwinfo *hwinfo,
                              Uint32 min_group_size)
{
  if (hwinfo->num_virt_l3_caches == 1)
  {
    return false;
  }
  Uint32 largest_group_id = RNIL;
  Uint32 largest_group_size = 0;
  Uint32 group_size = min_group_size;
  DEBUG_HW((stderr,
            "create_min_virt_l3_cache_list\n"));
  DEBUG_HW((stderr,
            "Min Group size is: %u\n", group_size));
  /**
   * When we arrive here, no groups should be larger than the min_group_size
   * and at least two groups still exists that we can create into a new merged
   * group. This new merged group might not be of minimum and the combined
   * group should never be created bigger than the minimum group size.
   */
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    DEBUG_HW((stderr,
              "Group[%u]: %u\n", i, g_num_virt_l3_cpus[i]));
    if (g_num_virt_l3_cpus[i] < group_size &&
        g_num_virt_l3_cpus[i] > largest_group_size)
    {
      largest_group_size = g_num_virt_l3_cpus[i];
      largest_group_id = i;
    }
  }
  require(largest_group_id != RNIL);
  Uint32 sec_largest_group_id = RNIL;
  Uint32 sec_largest_group_size = 0;
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    if (i != largest_group_id &&
        g_num_virt_l3_cpus[i] < group_size &&
        g_num_virt_l3_cpus[i] > sec_largest_group_size)
    {
      sec_largest_group_size = g_num_virt_l3_cpus[i];
      sec_largest_group_id = i;
    }
  }
  DEBUG_HW((stderr,
            "Largest Group[%u] = %u: SL Group[%u] = %u to be merged\n",
             largest_group_id,
             largest_group_size,
             sec_largest_group_id,
             sec_largest_group_size));
  if (sec_largest_group_id == RNIL)
  {
    return true;
  }
  require(sec_largest_group_id != RNIL);
  require(largest_group_id != RNIL);
  merge_virt_l3_cache_list(hwinfo,
                           largest_group_id,
                           sec_largest_group_id,
                           min_group_size);
  return true;
}


/**
 * We enter this function with the real map of L3 caches.
 * We try to create a virtual L3 cache list based on this.
 * This means that if some L3 cache group is too small,
 * we will merge it with another small group.
 * If the number of CPUs in the L3 cache group is too
 * high we divide it into a number of smaller L3 cache
 * groups.
 */
static int
create_virt_l3_cache_list(struct ndb_hwinfo *hwinfo,
                          Uint32 optimal_group_size,
                          Uint32 min_group_size,
                          Uint32 max_num_groups,
                          Uint32 num_query_instances)
{
  create_init_virt_l3_cache_list(hwinfo);
  if(num_query_instances == 0 && max_num_groups == 0)
    return 0;

  /**
   * We start by attempting to create a number of L3 cache groups
   * that all can contain the optimum sized groups. As soon as we find
   * enough of those we complete our search.
   *
   * If we can't find enough we will split the largest L3 cache group
   * into two groups. We will however only split of the largest
   * group will be split into at least two groups of optimal size.
   *
   * The function check_if_virt_l3_cache_will_be_ok checks if we can
   * create enough optimally sized groups, if we can't we step down
   * one step and try again. The last check is at the minimum sized
   * groups. If this step doesn't work we will still go through and
   * split L3 cache groups until there are no groups larger than the
   * minimum size.
   *
   * The final step is to start merging smaller groups such that they
   * can be of at least minimum group size. This step creates groups
   * that span more than one L3 cache group. This is not desirable
   * and is thus the last step when nothing else works to create a
   * set of minimally sized groups.
   */
  if (min_group_size < MIN_RR_GROUP_SIZE)
  {
    /* In this case use group size = 1 */
    min_group_size = 1;
    optimal_group_size = 1;
  }
  bool found_group_size = false;
  Uint32 used_group_size = min_group_size;
  Uint32 used_num_groups = max_num_groups;
  for (Uint32 check_group_size = optimal_group_size;
       check_group_size >= min_group_size;
       check_group_size -= 2)
  {
    Uint32 num_groups =
      (num_query_instances + (check_group_size - 1)) /
        check_group_size;
    if (num_groups * (check_group_size - 1) < num_query_instances)
    {
      if (check_if_virt_l3_cache_is_ok(hwinfo,
                                       check_group_size,
                                       num_groups,
                                       num_query_instances,
                                       min_group_size,
                                       true))
      {
        DEBUG_HW((stderr,
                  "Virtual L3 cache will be ok with group size %u\n",
                  check_group_size));
        used_group_size = check_group_size;
        used_num_groups = num_groups;
        found_group_size = true;
        break;
      }
    }
  }
  Uint32 loop_count = 0;
  do
  {
    if (check_if_virt_l3_cache_is_ok(hwinfo,
                                     used_group_size,
                                     used_num_groups,
                                     num_query_instances,
                                     min_group_size,
                                     false))
    {
      return used_num_groups;
    }
    DEBUG_HW((stderr, "Split virtual L3 cache list\n"));
    if (!split_virt_l3_cache_list(hwinfo,
                                  used_group_size))
    {
      break;
    }
    loop_count++;
    require(loop_count < 100); //Make sure we don't enter eternal loop
  } while (true);
  require(!found_group_size);
  /**
   * The only way to come here is if we have split all L3 cache groups into
   * the smallest possible size. Still we haven't been able to create enough
   * L3 cache groups. At this point we cannot continue without creating
   * some Round Robin groups that spans more than one L3 cache.
   *
   * We minimise the number of Round Robin groups that use more than one L3
   * cache by merging the largest remaining virtual L3 cache groups that
   * are not at least of the minimal size.
   *
   * If not even this is possible we will start by merging the smallest
   * L3 cache groups to be able to form groups of at least the minimum
   * group size.
   */
  DEBUG_HW((stderr,
            "Start merge loop, min_group_size: %u, max_num_groups: %u, "
            "num_query_instances: %u\n",
            min_group_size,
            max_num_groups,
            num_query_instances));
  loop_count = 0;
  do
  {
    if (check_if_virt_l3_cache_is_ok(hwinfo,
                                     min_group_size,
                                     max_num_groups,
                                     num_query_instances,
                                     min_group_size,
                                     false))
    {
      return max_num_groups;
    }
    DEBUG_HW((stderr,
              "Merge entries in the virtual L3 cache list"
              ", minimum group size is %u\n",
              min_group_size));
    if (!create_min_virt_l3_cache_list(hwinfo,
                                       min_group_size))
    {
      break;
    }
    loop_count++;
    require(loop_count < 10); //Make sure we don't enter eternal loop
  } while (true);
  require(false);
  return 0;
}

Uint32
Ndb_CreateCPUMap(Uint32 num_query_instances, Uint32 max_threads)
{
  struct ndb_hwinfo *hwinfo = g_ndb_hwinfo;
  /**
   * We have set up HW information and now we need to set up the CPU map
   * to make it easy to assign CPUs to the various threads.
   * We will return the number of Round Robin groups from this method.
   * 
   * This code is based on finding a set of Round Robin groups. The
   * special case of 0 LDM instances requires no Round Robin groups,
   * but the code is designed to create Round Robin groups. The special
   * case of 0 LDM threads is easiest by creating 1 Round Robin group.
   * The upper level will simply use this to create a list of CPUs and
   * this list will be ok even if we set number of LDM instances to 0
   * here.
   */
  g_create_cpu_map = (Uint32*)malloc(sizeof(Uint32) * max_threads);
  num_query_instances = (num_query_instances == 0) ? 1 : num_query_instances;
  Uint32 optimal_group_size = MAX_RR_GROUP_SIZE;
  Uint32 min_group_size = MIN(MIN_RR_GROUP_SIZE, num_query_instances);
  Uint32 max_num_groups =
    (num_query_instances + (min_group_size - 1)) / min_group_size;

  DEBUG_HW((stderr,
            "Call create_virt_l3_cache_list: "
            " opt group size: %u\n"
            " max_num_groups: %u min_group_size: %u\n"
            " num query instances: %u\n",
            optimal_group_size,
            max_num_groups,
            min_group_size,
            num_query_instances));
  Uint32 num_rr_groups = create_virt_l3_cache_list(hwinfo,
                                                   optimal_group_size,
                                                   min_group_size,
                                                   max_num_groups,
                                                   num_query_instances);
  sort_virt_l3_caches(hwinfo);
  adjust_rr_group_sizes(hwinfo,
                        num_rr_groups,
                        num_query_instances);
  create_prev_list(hwinfo);
  create_cpu_list(hwinfo,
                  num_rr_groups,
                  num_query_instances);
  return num_rr_groups;
}

static struct ndb_hwinfo *Ndb_SetHWInfo()
{
  if (!inited || ncpu == 0)
  {
    perror("Not inited");
    abort();
  }

  const size_t sz_hwinfo = sizeof(struct ndb_hwinfo);
  const size_t sz_cpuinfo = ncpu * sizeof(ndb_cpuinfo_data);
  const size_t sz_cpudata = ncpu * sizeof(ndb_cpudata);

  void * p_hwinfo = malloc(sz_hwinfo);
  if (p_hwinfo == nullptr)
  {
    perror("malloc failed");
    abort();
  }
  memset(p_hwinfo, 0, sz_hwinfo);
  g_ndb_hwinfo = (struct ndb_hwinfo*)p_hwinfo;

  void * p_cpuinfo = malloc(sz_cpuinfo);
  if (p_cpuinfo == nullptr)
  {
    perror("malloc failed");
    abort();
    return nullptr;
  }
  memset(p_cpuinfo, 0, sz_cpuinfo);

  void * p_cpudata = malloc(sz_cpudata);
  if (p_cpudata == nullptr)
  {
    perror("malloc failed");
    abort();
    return nullptr;
  }
  memset(p_cpudata, 0, sz_cpudata);

  struct ndb_hwinfo* res = (struct ndb_hwinfo*)p_hwinfo;
  res->cpu_info = (ndb_cpuinfo_data*)p_cpuinfo;
  res->cpu_data = (ndb_cpudata*)p_cpudata;
  res->cpu_cnt_max = ncpu;
  res->cpu_cnt = ncpu;
  res->total_cpu_capacity = ncpu * 100;

  for (Uint32 i = 0; i < ncpu; i++)
  {
    g_ndb_hwinfo->cpu_info[i].socket_id = Uint32(~0);
    g_ndb_hwinfo->cpu_info[i].package_id = Uint32(~0);
    g_ndb_hwinfo->cpu_info[i].core_id = Uint32(~0);
    g_ndb_hwinfo->cpu_info[i].l3_cache_id = Uint32(~0);
    g_ndb_hwinfo->cpu_info[i].prev_virt_l3_cpu_map = RNIL;
    g_ndb_hwinfo->cpu_info[i].next_virt_l3_cpu_map = RNIL;
    g_ndb_hwinfo->cpu_info[i].next_cpu_map = RNIL;
    g_ndb_hwinfo->cpu_info[i].online = 0;
    g_ndb_hwinfo->cpu_info[i].cpu_no = i;
#ifdef _WIN32
    g_ndb_hwinfo->cpu_info[i].group_number = Uint32(~0);
    g_ndb_hwinfo->cpu_info[i].group_index = Uint32(~0);
#endif
    g_ndb_hwinfo->cpu_info[i].cpu_capacity = 1;
  }

  if (init_hwinfo(res) || Ndb_ReloadHWInfo(res))
  {
    perror("init_hwinfo or Ndb_ReloadHWInfo failed");
    abort();
    return nullptr;
  }
  if (init_cpudata(res) || Ndb_ReloadCPUData(res))
  {
    free(p_cpudata);
    res->is_cpudata_available = false;
    res->cpu_data = nullptr;
  }
  if (res->is_cpuinfo_available)
  {
    if (create_l3_cache_list(res))
    {
      res->is_cpuinfo_available = false;
    }
  }
  if (!res->is_cpuinfo_available)
  {
    free(p_cpuinfo);
    res->is_cpuinfo_available = false;
    res->cpu_info = nullptr;
  }
  if (!res->is_cpudata_available)
  {
    free(p_cpudata);
    res->is_cpudata_available = false;
    res->cpu_data = nullptr;
  }
  return res;
}

struct ndb_hwinfo *Ndb_GetHWInfo(bool get_data)
{
  struct ndb_hwinfo *res = g_ndb_hwinfo;
  if (get_data && res->is_cpudata_available)
  {
    if (init_cpudata(res) || Ndb_ReloadCPUData(res))
    {
      ; //Ignore errors here
    }
  }
  return res;
}

static void Ndb_FreeHWInfo()
{
  struct ndb_hwinfo *p = g_ndb_hwinfo;
  if (p == nullptr)
    return;
  struct ndb_cpuinfo_data *cpuinfo_data = p->cpu_info;
  struct ndb_cpudata *cpudata = p->cpu_data;
  free((void*)cpuinfo_data);
  free((void*)cpudata);
  free((void*)p);
  g_ndb_hwinfo = nullptr;
}

Uint32 Ndb_getCPUL3CacheId(Uint32 cpu_id)
{
  struct ndb_hwinfo *hwinfo = g_ndb_hwinfo;
  if (!hwinfo->is_cpuinfo_available)
  {
    return 0;
  }
  struct ndb_cpuinfo_data *cpu_info = &hwinfo->cpu_info[cpu_id];
  if (cpu_id > hwinfo->cpu_cnt_max)
  {
    return 0;
  }
  if (!cpu_info->online)
  {
    return 0;
  }
  return cpu_info->l3_cache_id;
}

#if defined(HAVE_SOLARIS_AFFINITY) || \
    defined(HAVE_LINUX_SCHEDULING) || \
    defined(HAVE_CPUSET_SETAFFINITY)
static void
check_cpu_online(struct ndb_hwinfo *hwinfo)
{
  Uint32 total_cpu_capacity = 0;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    if (!NdbThread_IsCPUAvailable(i))
    {
      if (hwinfo->cpu_info != nullptr && hwinfo->cpu_info[i].online == 1)
      {
        hwinfo->cpu_info[i].online = 0;
        if (hwinfo->cpu_cnt == 0)
          abort();
        hwinfo->cpu_cnt--;
      }
      else
      {
        if (hwinfo->cpu_info != nullptr)
        {
          total_cpu_capacity += hwinfo->cpu_info[i].cpu_capacity;
        }
      }
    }
  }
  if (total_cpu_capacity > 0)
  {
    /**
     * Only count in full CPUs (capacity == 100)
     */
    total_cpu_capacity /= 100;
    total_cpu_capacity *= 100;
    hwinfo->total_cpu_capacity = total_cpu_capacity;
  }
  DEBUG_HW((stderr,
            "There are %u CPUs online after checking,"
            " total CPU capacity %u\n",
            hwinfo->cpu_cnt,
            hwinfo->total_cpu_capacity));
}
#endif

#ifdef _WIN32
/**
 * Windows HW Information
 * ----------------------
 * We use GetLogicalProcessorInformationEx to get information about the
 * HW we have under us. This means that we also have access to the
 * Processor Group information. This makes it possible to automatically
 * lock threads to all CPUs in the machine, even if we have more than
 * 64 CPUs in the Windows box.
 */

#include <windows.h>
#include <tchar.h>

typedef BOOL (WINAPI *LPFN_GLPI)(
    LOGICAL_PROCESSOR_RELATIONSHIP,
    PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX,
    PDWORD);

static bool get_bit_kaffinity(KAFFINITY mask, Uint32 bit_no)
{
  Uint64 loc_mask = Uint64(Uint64(1) << bit_no) & Uint64(mask);
  if (loc_mask == 0)
    return false;
  return true;
}

static PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX
get_processor_data(LOGICAL_PROCESSOR_RELATIONSHIP relationship,
                   DWORD & buf_len)
{
  LPFN_GLPI glpi;
  glpi = (LPFN_GLPI) GetProcAddress(
                          GetModuleHandle(TEXT("kernel32")),
                          "GetLogicalProcessorInformationEx");
  if (nullptr == glpi)
  {
    return nullptr;
  }

  bool done = false;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX buf = nullptr;
  while (!done)
  {
    DWORD res = glpi(relationship, buf, &buf_len);

    if (!res)
    {
      if (GetLastError() == ERROR_INSUFFICIENT_BUFFER)
      {
        if (buf != nullptr)
        {
          free(buf);
        }

        buf = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)
           malloc(buf_len);

        if (buf == nullptr)
        {
          return nullptr;
        }
      }
      else
      {
        return nullptr;
      }
    }
    else
    {
       done = true;
    }
  }
  return buf;
}

/**
 * Create mapping from GroupNumber and id in Group to cpu_no.
 * Windows creates CPU groups, one cannot lock a thread to
 * more than one CPU group. Our automatic CPU locking will
 * however never lock a thread outside its own CPU core or
 * at least not outside its L3 cache, and these always reside
 * in the same CPU group in Windows.
 */
int set_num_groups(struct ndb_hwinfo *hwinfo)
{
  DWORD buf_len = 0;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX buf = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX ptr = nullptr;

  buf = get_processor_data(RelationProcessorCore, buf_len);
  if (buf == nullptr)
    return -1;
  ptr = buf;
  Uint32 cpu_no = 0;
  Uint32 byte_offset = 0;
  while (byte_offset < buf_len)
  {
    require(ptr->Relationship == RelationProcessorCore);
    PROCESSOR_RELATIONSHIP *processor = &ptr->Processor;
    GROUP_AFFINITY *group_aff = &processor->GroupMask[0];
    Uint32 group_number = group_aff->Group;
    KAFFINITY mask = group_aff->Mask;
    for (Uint32 cpu_index = 0; cpu_index < 64; cpu_index++)
    {
      if (get_bit_kaffinity(mask, cpu_index))
      {
        hwinfo->cpu_info[cpu_no].group_number = group_number;
        hwinfo->cpu_info[cpu_no].group_index = cpu_index;
        cpu_no++;
      }
      require(cpu_no <= hwinfo->cpu_cnt_max);
    }
    byte_offset += ptr->Size;
    char *new_ptr = (char*)buf;
    new_ptr += byte_offset;
    ptr = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)new_ptr;
  }
  free(buf);
  hwinfo->cpu_cnt = cpu_no;
  return 0;
}

static
Uint32 get_cpu_number(struct ndb_hwinfo *hwinfo,
                      Uint32 group_number,
                      Uint32 cpu_index)
{
  for (Uint32 cpu_no = 0; cpu_no < hwinfo->cpu_cnt; cpu_no++)
  {
    if (hwinfo->cpu_info[cpu_no].group_number == group_number &&
        hwinfo->cpu_info[cpu_no].group_index == cpu_index)
    {
      return cpu_no;
    }
  }
  perror("Failed to find CPU in CPU group");
  abort();
  return Uint32(~0);
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo *hwinfo)
{
  if (set_num_groups(hwinfo) == (int)-1)
  {
    return -1;
  }

  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX buf = nullptr;
  PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX ptr = nullptr;
  DWORD buf_len = 0;

  buf = get_processor_data(RelationAll, buf_len);
  if (buf == nullptr)
  {
    return -1;
  }
  ptr = buf;
  Uint32 l3_cache_id = 0;
  Uint32 socket_id = 0;
  Uint32 core_id = 0;
  Uint32 byte_offset = 0;
  Uint32 min_cpus_per_core = 4;
  while (byte_offset < buf_len)
  {
    switch (ptr->Relationship)
    {
      case RelationProcessorCore:
      {
        PROCESSOR_RELATIONSHIP *processor = &ptr->Processor;
        GROUP_AFFINITY *group_aff = &processor->GroupMask[0];
        Uint32 group_number = group_aff->Group;
        KAFFINITY mask = group_aff->Mask;
        Uint32 cpus_per_core = 0;
        for (Uint32 cpu_index = 0; cpu_index < 64; cpu_index++)
        {
          if (get_bit_kaffinity(mask, cpu_index))
          {
            Uint32 cpu_no = get_cpu_number(hwinfo, group_number, cpu_index);
            hwinfo->cpu_info[cpu_no].core_id = core_id;
            cpus_per_core++;
          }
        }
        min_cpus_per_core = MIN(cpus_per_core, min_cpus_per_core);
        core_id++;
        break;
      }
      case RelationCache:
      {
        CACHE_RELATIONSHIP *cache_desc = &ptr->Cache;
        if (cache_desc->Level == 3)
        {
          GROUP_AFFINITY *group_aff = &cache_desc->GroupMask;
          KAFFINITY mask = group_aff->Mask;
          Uint32 group_number = group_aff->Group;
          for (Uint32 cpu_index = 0; cpu_index < 64; cpu_index++)
          {
            if (get_bit_kaffinity(mask, cpu_index))
            {
              Uint32 cpu_no = get_cpu_number(hwinfo, group_number, cpu_index);
              hwinfo->cpu_info[cpu_no].l3_cache_id = l3_cache_id;
            }
          }
          l3_cache_id++;
        }
        break;
      }
      case RelationProcessorPackage:
      {
        // Logical processors share a physical package.
        PROCESSOR_RELATIONSHIP *processor = &ptr->Processor;
        Uint32 group_count = processor->GroupCount;
        for (Uint32 group = 0; group < group_count; group++)
        {
          GROUP_AFFINITY *group_aff = &processor->GroupMask[group];
          Uint32 group_number = group_aff->Group;
          KAFFINITY mask = group_aff->Mask;
          for (Uint32 cpu_index = 0; cpu_index < 64; cpu_index++)
          {
            if (get_bit_kaffinity(mask, cpu_index))
            {
              Uint32 cpu_no = get_cpu_number(hwinfo, group_number, cpu_index);
              hwinfo->cpu_info[cpu_no].socket_id = socket_id;
              hwinfo->cpu_info[cpu_no].package_id = socket_id;
            }
          }
        }
        socket_id++;
        break;
      }
      default:
        break;
    }
    byte_offset += ptr->Size;
    char *new_ptr = (char*)buf;
    new_ptr += byte_offset;
    ptr = (PSYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX)new_ptr;
  }
  free(buf);

  MEMORYSTATUSEX mem_status;
  mem_status.dwLength = sizeof (mem_status);
  GlobalMemoryStatusEx(&mem_status);

  hwinfo->is_cpuinfo_available = 0;
  hwinfo->is_cpudata_available = 0;
  hwinfo->num_cpu_cores = core_id;
  hwinfo->num_cpu_sockets = socket_id;
  hwinfo->num_cpu_per_core = min_cpus_per_core;
  hwinfo->num_shared_l3_caches = l3_cache_id;
  hwinfo->cpu_model_name[0] = 0;
  hwinfo->hw_memory_size = mem_status.ullTotalPhys;
  if (hwinfo->cpu_cnt_max < hwinfo->cpu_cnt)
  {
    hwinfo->cpu_cnt = hwinfo->cpu_cnt_max;
  }
  return 0;
}

static int NdbHW_Init_platform()
{
  return 0;
}

static void NdbHW_End_platform()
{
}

static int init_hwinfo(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int init_cpudata(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int Ndb_ReloadCPUData(struct ndb_hwinfo *hwinfo)
{
  (void)hwinfo;
  return 0;
}

#elif defined(HAVE_MAC_OS_X_THREAD_INFO)
/**
 * Mac OS X HW Information
 * -----------------------
 *
 * On Mac OS X we can retrieve information using sysctlbyname for the
 * following information:
 * 1) hw.activecpu
 *    Number of active processors in system
 * 2) hw.physicalcpu_max
 *    Number of CPU cores in system
 * 3) hw.packages
 *    Number of CPU sockets
 * 4) hw.memsize
 *    Memory size of HW
 * 5) machdep.cpu.brand_string
 *    Model name of CPU
 */
#include <sys/types.h>
#include <sys/sysctl.h>

static int NdbHW_Init_platform()
{
  return 0;
}

static void NdbHW_End_platform()
{
}

static int init_hwinfo(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int init_cpudata(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo * hwinfo)
{
  int res;
  Int32 active_cpu;
  Int32 cpu_cores;
  Int32 cpu_sockets;
  Int64 memory_size;
  char brand_buf[128];

  size_t size_var = sizeof(active_cpu);
  res = sysctlbyname("hw.activecpu",
                     (void*)&active_cpu,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(active_cpu))
    goto error_exit;

  size_var = sizeof(cpu_cores);
  res = sysctlbyname("hw.physicalcpu_max",
                     (void*)&cpu_cores,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(cpu_cores))
    goto error_exit;

  size_var = sizeof(cpu_sockets);
  res = sysctlbyname("hw.packages",
                     (void*)&cpu_sockets,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(cpu_sockets))
    goto error_exit;

  size_var = sizeof(memory_size);
  res = sysctlbyname("hw.memsize",
                     (void*)&memory_size,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(memory_size))
    goto error_exit;

  size_var = sizeof(brand_buf) - 1;
  res = sysctlbyname("machdep.cpu.brand_string",
                     (void*)&brand_buf[0],
                     &size_var,
                     nullptr,
                     0);
  if (res != 0)
    goto error_exit;

  hwinfo->cpu_cnt = active_cpu;
  hwinfo->num_cpu_cores = cpu_cores;
  hwinfo->num_cpu_sockets = cpu_sockets;
  hwinfo->hw_memory_size = (Uint64)memory_size;
  memcpy(hwinfo->cpu_model_name, brand_buf, size_var);
  hwinfo->cpu_model_name[size_var] = 0; // Null terminate
  hwinfo->is_cpuinfo_available = 0;
  hwinfo->is_cpudata_available = 0;
  return 0;

error_exit:
  return -1;
}

static int Ndb_ReloadCPUData(struct ndb_hwinfo *hwinfo)
{
  (void)hwinfo;
  return 0;
}

#elif defined(HAVE_CPUSET_SETAFFINITY)

#include <sys/types.h>
#include <sys/sysctl.h>
/**
 * FreeBSD HW Information
 * ----------------------
 *
 * On FreeBSD we can retrieve information using sysctlbyname for the
 * following information:
 * 1) hw.ncpu
 *    Number of CPUs in system
 * 2) hw.physicalcpu_max
 *    Number of CPU cores in system
 * 3) hw.packages
 *    Number of CPU sockets
 * 4) hw.physmem
 *    Memory size of HW
 * 5) machdep.cpu.brand_string
 *    Model name of CPU
 */

static int NdbHW_Init_platform()
{
  return 0;
}

static void NdbHW_End_platform()
{
}

static int init_hwinfo(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int init_cpudata(struct ndb_hwinfo * hwinfo)
{
  (void)hwinfo;
  return 0;
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo * hwinfo)
{
  int res;
  Int32 active_cpu;
  Int64 memory_size;

  size_t size_var = sizeof(active_cpu);
  res = sysctlbyname("hw.ncpu",
                     (void*)&active_cpu,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(active_cpu))
    goto error_exit;

  size_var = sizeof(memory_size);
  res = sysctlbyname("hw.physmem",
                     (void*)&memory_size,
                     &size_var,
                     nullptr,
                     0);
  if (res != 0 || size_var != sizeof(memory_size))
    goto error_exit;

  if (res != 0)
    goto error_exit;

  for (Int32 i = 0; i < active_cpu; i++)
  {
    hwinfo->cpu_info[i].online = true;
  }
  hwinfo->cpu_cnt = active_cpu;
  check_cpu_online(hwinfo);
  hwinfo->num_cpu_cores = 0;
  hwinfo->num_cpu_sockets = 0;
  hwinfo->hw_memory_size = (Uint64)memory_size;
  hwinfo->cpu_model_name[0] = 0; // Null terminate
  hwinfo->is_cpuinfo_available = 0;
  hwinfo->is_cpudata_available = 0;
  return 0;

error_exit:
  return -1;
}

static int Ndb_ReloadCPUData(struct ndb_hwinfo *hwinfo)
{
  (void)hwinfo;
  return 0;
}

#elif defined (HAVE_LINUX_SCHEDULING)
/* Linux HW Information */

static inline Uint64 t2us(Uint64 ticks)
{
  return ticks * ticks_per_us;
}

static int NdbHW_Init_platform()
{
  return 0;
}

static void NdbHW_End_platform()
{
}

static int init_cpudata(struct ndb_hwinfo * hwinfo)
{
  /**
   * linux (currently/to the best of my knowledge)
   *   always enumerates it's CPU's 0-N
   */
  for (Uint32 i = 0; i < ncpu; i++)
  {
    hwinfo->cpu_data[i].cpu_no = i;
  }
  return 0;
}

const static size_t offsets[] =
{
  /**
   * This order must match that of /proc/stat
   */
  offsetof(struct ndb_cpudata,  cs_user_us),
  offsetof(struct ndb_cpudata,  cs_nice_us),
  offsetof(struct ndb_cpudata,  cs_sys_us),
  offsetof(struct ndb_cpudata,  cs_idle_us),
  offsetof(struct ndb_cpudata,  cs_iowait_us),
  offsetof(struct ndb_cpudata,  cs_irq_us),
  offsetof(struct ndb_cpudata,  cs_sirq_us),
  offsetof(struct ndb_cpudata,  cs_steal_us),
  offsetof(struct ndb_cpudata,  cs_guest_us),
  offsetof(struct ndb_cpudata,  cs_guest_nice_us),
  offsetof(struct ndb_cpudata,  cs_unknown1_us),
  offsetof(struct ndb_cpudata,  cs_unknown2_us)
};

static int Ndb_ReloadCPUData(struct ndb_hwinfo *hwinfo)
{
  if (!inited)
    return -1;

  const Uint32 max_cpu_no = hwinfo->cpu_cnt_max - 1;
  Uint32 curr_cpu = 0;

  FILE *stat_file = fopen("/proc/stat", "r");
  if (stat_file == nullptr)
  {
    perror("failed to open /proc/stat");
    return -1;
  }
  FileGuard g(stat_file); // close at end...

  for (Uint32 i = 0; i < ncpu; i++)
  {
    hwinfo->cpu_data[i].online = 0;
  }

  char buf[1024];
  char * p = &buf[0];
  char * c = nullptr;
  while (fgets(buf, sizeof(buf), stat_file))
  {
    if (curr_cpu > max_cpu_no || (c = strstr(p, "cpu")) == nullptr)
    {
      break;
    }
    if (c[3] == ' ')
    {
      /**
       * this is the aggregate...skip over that
       */
      continue;
    }
    // c + 3 should be a number
    char * endptr = nullptr;
    long val = strtol(c + 3, &endptr, 10);
    if (endptr == c + 3)
    {
      // no number found...
      return -1;
    }
    if (! (val >= 0 && val <= (long)max_cpu_no))
    {
      assert(false);
      return -1;
    }
    curr_cpu = val;

    Uint64 ticks[12];
    memset(ticks, 0, sizeof(ticks));
    sscanf(endptr,
           "%llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
           ticks+0,ticks+1,ticks+2,ticks+3,ticks+4,
           ticks+5,ticks+6,ticks+7,ticks+8,ticks+9,
           ticks+10,ticks+11);

    char * base = (char *)&hwinfo->cpu_data[curr_cpu];
    for (int i = 0; i < 12; i++)
    {
      Uint64 * ptr = (Uint64*)(base + offsets[i]);
      * ptr = t2us(ticks[i]);
    }
    curr_cpu++;
  }
  return 0;
}
static Uint32
get_cpu_atom_core(struct ndb_hwinfo *hwinfo)
{
  char buf[128];
  char read_buf[128];
  SparseBitmask mask(hwinfo->cpu_cnt_max);

  for (Uint32 type = 0; type < 2; type++)
  {
    if (type == 0)
    {
      snprintf(buf,
               sizeof(buf),
               "/sys/devices/cpu_atom/cpus");
    }
    else
    {
      snprintf(buf,
               sizeof(buf),
               "/sys/devices/cpu_core/cpus");
    }
    FILE *atom_core_info = fopen(buf, "r");

    if (atom_core_info == nullptr)
    {
      /**
       * No Intel Atom/Core processors, we assume that all CPUs are equal
       */
      return (type == 0) ? 1 : 50;
    }
    char error_buf[384];
    FileGuard g(atom_core_info); // close at end...
    if (fgets(read_buf, sizeof(read_buf), atom_core_info) == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to read %s", buf);
      perror(error_buf);
      return 0;
    }
    mask.clear();
    int res = ::parse_mask(read_buf, mask);
    if (res <= 0)
    {
      snprintf(error_buf,
               sizeof(error_buf),
               "Failed to parse %s from %s",
               read_buf,
               buf);
      perror(error_buf);
      return 0;
    }
    Uint32 start_bit = 0;
    do
    {
      Uint32 next_cpu = mask.find(start_bit);
      if (next_cpu == SparseBitmask::NotFound)
      {
        if (start_bit == 0)
        {
          /**
           * There is an Intel Atom/Core file, but no CPUs,
           * assume all are equal.
           */
          return (type == 0) ? 1 : 50;
        }
      }
      break;
      if (next_cpu >= hwinfo->cpu_cnt_max)
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU number %u higher than max %u",
                 next_cpu,
                 hwinfo->cpu_cnt_max);
        perror(error_buf);
        return 0;
      }
      /**
       * We set CPU capacity to half of Intel Core for Intel Atom
       * and to 100 for Intel Core.
       */
      hwinfo->cpu_info[next_cpu].cpu_capacity = (type == 0) ? 50 : 100;
    } while (1);
  }
  return 100;
}

static int
get_cpu_capacity(ndb_cpuinfo_data *cpuinfo, Uint32 cpu)
{
  char buf[256];
  char read_buf[256];
  snprintf(buf,
           sizeof(buf),
           "/sys/devices/system/cpu/cpu%u/cpu_capacity",
           cpu);
  FILE *capacity_info = fopen(buf, "r");
  if (capacity_info == nullptr)
  {
    DEBUG_HW((stderr,
              "Found no CPU capacity for CPU %u\n",
              cpu));
    return -1;
  }
  FileGuard g(capacity_info); // close at end...
  if (fgets(read_buf, sizeof(read_buf), capacity_info) == nullptr)
  {
    DEBUG_HW((stderr,
              "Failed to read CPU capacity for CPU %u\n",
              cpu));
    return -1;
  }
  Uint32 capacity;
  if (sscanf(read_buf, "%u", &capacity) == 1)
  {
    cpuinfo->cpu_capacity = capacity;
    DEBUG_HW((stderr,
              "Read CPU capacity %u for CPU %u\n",
              capacity,
              cpu));
  }
  else
  {
    DEBUG_HW((stderr,
              "Failed to sscanf CPU capacity for CPU %u\n",
              cpu));
    return -1;
  }
  return 0;
}

static void
get_cpu_throughput(struct ndb_hwinfo *hwinfo)
{

  Uint32 cpu_capacity = 0;
  Uint32 max_cpu_capacity = 0;
  Uint32 i;
  for (i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    /**
     * First check if we get cpu_capacity file (usually found on ARMs)
     */
    struct ndb_cpuinfo_data *cpuinfo = &hwinfo->cpu_info[i];
    if (cpuinfo->online)
    {
      if (cpu_capacity == 0)
      {
        if (get_cpu_capacity(cpuinfo, i) != 0)
          break;
        cpu_capacity = cpuinfo->cpu_capacity;
      }
      else
      {
        if (get_cpu_capacity(cpuinfo, i) != 0)
        {
          char error_buf[256];
          snprintf(error_buf,
                   sizeof(error_buf),
                   "Failed to read CPU capacity after success, cpu: %u",
                   i);
          perror(error_buf);
          abort();
          return;
        }
      }
      max_cpu_capacity = MAX(max_cpu_capacity,
                             cpuinfo->cpu_capacity);
    }
  }
  if (i == hwinfo->cpu_cnt_max)
  {
    ;
  }
  else
  {
    /**
     * Could not find any cpu_capacity file
     * Check if it is Intel using Atom + Core
     * If it isn't we assume all CPUs are equal and they
     * will below all be set to throughput of 100 since
     * they are initialised to 1.
     */
    max_cpu_capacity = get_cpu_atom_core(hwinfo);
    require(max_cpu_capacity > 0);
  }   
  /**
   * Standardise on highest throughput = 100
   * CPU capacity is set to 0 for offline CPUs.
   */
  bool is_mixed_core_throughput = false;
  for (i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    struct ndb_cpuinfo_data *cpuinfo = &hwinfo->cpu_info[i];
    if (cpuinfo->online)
    {
      Uint32 capacity = (cpuinfo->cpu_capacity * 100) /
                        max_cpu_capacity;
      if (capacity != 100)
      {
        /**
         * For the moment that the Power efficient has half the
         * throughput compared to the efficiency cores is a good
         * estimation. In reality benchmarks has shown that it is
         * around 45%. However setting it to 50% instead makes it
         * easy to handle mapping threads to CPUs.
         *
         * In an environment like Intel Alder Lake and Intel
         * Raptor Lake the efficiency cores are hyperthreaded, thus
         * when both are used the two CPUs within this one core
         * has about the same throughput as two Power efficient
         * cores. The automatic mapping will always try to use
         * efficiency cores for LDM threads which benefits from
         * hyperthreading (in the sense that one CPU could make
         * use of almost the entire CPU core). Thus the LDM
         * threads are using the CPU core to its full measure
         * both with 1 and 2 threads active in the CPU core.
         *
         * Thus the other thread types are more likely to run
         * equally well on both efficiency cores and power
         * efficient cores.
         *
         * Apple M1 Pro (running on Linux) is an example of
         * a CPU architecture where no hyperthreading is used,
         * but also in this case the throughput of Power
         * efficient cores is about half of the throughput of
         * efficiency cores.
         *
         * So assuming we run on a CPU with 3 efficiency cores
         * and 2 Power efficient cores, we get a total capacity
         * of 400. Thus one manner of handling this is to use
         * automatic thread management with 4 CPUs. These will
         * be 2 LDM threads, 1 main thread and 1 receive thread.
         *
         *  In this case we will provide 2 efficiency cores for
         * LDM thread, another efficiency cores for the main
         * thread and finally the single recv thread will notice
         * that it has only Power efficient cores to choose from.
         * Thus it will choose to use two receive threads instead
         * using the two power efficient cores.
         */
        is_mixed_core_throughput = true;
        capacity = 50;
      }
      cpuinfo->cpu_capacity = capacity;
    }
    else
    {
      cpuinfo->cpu_capacity = 0;
    }
    require(cpuinfo->cpu_capacity <= 100);
  }
  hwinfo->is_mixed_core_throughput = is_mixed_core_throughput;
  DEBUG_HW((stderr,
            "There are%s mixed CPU cores\n",
            is_mixed_core_throughput ? "" : " no"));
}

static int
get_meminfo(struct ndb_hwinfo *hwinfo)
{
  char buf[1024];
  FILE * meminfo = fopen("/proc/meminfo", "r");
  if (meminfo == nullptr)
  {
    perror("failed to open /proc/meminfo");
    return -1;
  }

  FileGuard g(meminfo); // close at end...

  Uint64 memory_size_kb = 0;
  while (fgets(buf, sizeof(buf), meminfo))
  {
    Uint64 val;
    if (sscanf(buf, "MemTotal: %llu", &val) == 1)
    {
      memory_size_kb = val; // /proc/meminfo reports MemTotal in kb
      break;
    }
  }
  if (memory_size_kb == 0)
  {
    perror("Found no MemTotal in /proc/meminfo");
    return -1;
  }
  DEBUG_HW((stderr,
            "Total memory is %llu MBytes\n",
            memory_size_kb / 1024));
  /* Memory is in kBytes */
  hwinfo->hw_memory_size = memory_size_kb * 1024; // hw_memory_size in bytes
  return 0;
}

static int init_hwinfo(struct ndb_hwinfo* hwinfo)
{
  hwinfo->is_cpuinfo_available = 1;
  hwinfo->is_cpudata_available = 1;
  return 0;
}

static int
get_core_siblings_info(struct ndb_hwinfo *hwinfo)
{
  char error_buf[612];
  SparseBitmask mask(hwinfo->cpu_cnt_max);
  Uint32 next_core_id = 0;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    if (hwinfo->cpu_info[i].core_id != Uint32(~0))
    {
      /**
       * Already handled this CPU since it was included in previous
       * CPUs common cache CPUs.
       */
      continue;
    }
    char buf[256];
    snprintf(buf,
             sizeof(buf),
             "/sys/devices/system/cpu/cpu%u/topology/core_cpus_list",
             i);
    FILE *sibl_info = fopen(buf, "r");
    if (sibl_info == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to open %s", buf);
      perror(error_buf);
      return -1;
    }
    FileGuard g(sibl_info); // close at end...
    char read_buf[256];
    if (fgets(read_buf, sizeof(read_buf), sibl_info) == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to read %s", buf);
      perror(error_buf);
      return -1;
    }
    hwinfo->cpu_info[i].core_id = next_core_id;
    mask.clear();
    int res = ::parse_mask(read_buf, mask);
    if (res <= 0)
    {
      snprintf(error_buf,
               sizeof(error_buf),
               "Failed to parse %s from %s",
               read_buf,
               buf);
      perror(error_buf);
      return -1;
    }
    Uint32 start_bit = 0;
    do
    {
      Uint32 next_cpu = mask.find(start_bit);
      if (next_cpu == SparseBitmask::NotFound)
      {
        if (start_bit == 0)
        {
          snprintf(error_buf, sizeof(error_buf), "No bits set in %s", buf);
          perror(error_buf);
          return -1;
        }
        break;
      }
      if (next_cpu >= hwinfo->cpu_cnt_max)
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU number %u higher than max %u",
                 next_cpu,
                 hwinfo->cpu_cnt_max);
        perror(error_buf);
        return -1;
      }
      hwinfo->cpu_info[next_cpu].core_id = next_core_id;
      DEBUG_HW((stderr,
                "CPU %u found in core %u\n",
                next_cpu,
                next_core_id));
      start_bit = next_cpu + 1;
    } while (true);
    next_core_id++;
  }
  return next_core_id;
}

/**
 * On Linux ARM we don't get CPU socket information from /proc/cpuinfo, so
 * get it from /sys/devices/system/cpu/cpu0/topology/physica_package_id
 * instead. The package id is not necessarily starting at 0, so we need to
 * convert it into a socket id that starts at 0.
 *
 * On Linux ARM we don't have access to L3 cache information, so we
 * provide the socket id as the L3 cache id which should be a good estimate
 * although not perfect.
 */
static int
get_physical_package_ids(struct ndb_hwinfo* hwinfo)
{
  char error_buf[512];
  Uint32 num_cpu_sockets = 0;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    if (hwinfo->cpu_info[i].socket_id != Uint32(~0))
    {
      /* Already assigned socket_id */
      continue;
    }
    char buf[256];
    snprintf(buf,
             sizeof(buf),
             "/sys/devices/system/cpu/cpu%u/topology/physical_package_id",
             i);
    FILE *package_info = fopen(buf, "r");
    if (package_info == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to open %s", buf);
      perror(error_buf);
      return -1;
    }
    FileGuard g(package_info); // close at end...
    char read_buf[256];
    if (fgets(read_buf, sizeof(read_buf), package_info) == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to read %s", buf);
      perror(error_buf);
      return -1;
    }
    Uint32 package_id = (Uint32)strtol(read_buf, nullptr, 10);
    int err_code = errno;
    if (package_id == 0 && (err_code == EINVAL || err_code == ERANGE))
    {
      snprintf(error_buf,
               sizeof(error_buf),
               "Failed to convert %s into number",
               buf);
      perror(error_buf);
      return -1;
    }
    Uint32 socket_id = Uint32(~0);
    int max_socket_id = -1;
    for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
    {
      if (hwinfo->cpu_info[i].package_id == package_id)
      {
        socket_id = hwinfo->cpu_info[i].socket_id;
        break;
      }
      if (hwinfo->cpu_info[i].socket_id != Uint32(~0) &&
          (int)hwinfo->cpu_info[i].socket_id > max_socket_id)
      {
        max_socket_id = hwinfo->cpu_info[i].socket_id;
      }
    }
    if (socket_id == Uint32(~0))
    {
      if (max_socket_id == (int)-1)
      {
        socket_id = 0;
      }
      else
      {
        socket_id = max_socket_id + 1;
      }
    }
    hwinfo->cpu_info[i].package_id = package_id;
    hwinfo->cpu_info[i].socket_id = socket_id;
    hwinfo->cpu_info[i].l3_cache_id = socket_id;

    if (socket_id == num_cpu_sockets)
      num_cpu_sockets++;
  }
  hwinfo->num_shared_l3_caches = num_cpu_sockets;
  hwinfo->num_cpu_sockets = num_cpu_sockets;
  return 0;
}

static int
get_l3_cache_info(struct ndb_hwinfo *hwinfo)
{
  char error_buf[512];
  SparseBitmask mask(hwinfo->cpu_cnt_max);
  Uint32 next_l3_id = 0;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    hwinfo->cpu_info[i].l3_cache_id = Uint32(~0);
  }
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    if (!hwinfo->cpu_info[i].online)
    {
      /* CPU not online, ignore */
      continue;
    }
    if (hwinfo->cpu_info[i].l3_cache_id != Uint32(~0))
    {
      /**
       * Already handled this CPU since it was included in previous
       * CPUs common cache CPUs.
       */
      continue;
    }
    char buf[256];
    snprintf(buf,
             sizeof(buf),
             "/sys/devices/system/cpu/cpu%u/cache/index3/shared_cpu_list",
             i);
    FILE *cache_info = fopen(buf, "r");
    if (cache_info == nullptr)
    {
      return -1;
    }
    FileGuard g(cache_info); // close at end...
    char read_buf[256];
    if (fgets(read_buf, sizeof(read_buf), cache_info) == nullptr)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to read %s", buf);
      perror(error_buf);
      return -1;
    }
    hwinfo->cpu_info[i].l3_cache_id = next_l3_id;
    DEBUG_HW((stderr,
              "Found CPU %u in L3 cache region %u\n",
              i,
              next_l3_id));
    mask.clear();
    int res = ::parse_mask(read_buf, mask);
    if (res <= 0)
    {
      snprintf(error_buf, sizeof(error_buf), "Failed to parse %s", read_buf);
      perror(error_buf);
      return -1;
    }
    Uint32 start_bit = 0;
    do
    {
      Uint32 next_cpu = mask.find(start_bit);
      if (next_cpu == SparseBitmask::NotFound)
      {
        if (start_bit == 0)
        {
          perror("No bits set, should not be possible");
          return -1;
        }
        break;
      }
      if (next_cpu >= hwinfo->cpu_cnt_max)
      {
        perror("Found a non-existent CPU in CPU list");
        return -1;
      }
      hwinfo->cpu_info[next_cpu].l3_cache_id = next_l3_id;
      DEBUG_HW((stderr,
                "Found CPU %u in L3 cache region %u\n",
                next_cpu,
                next_l3_id));
      start_bit = next_cpu + 1;
    } while (true);
    next_l3_id++;
  }
  return next_l3_id;
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo * hwinfo)
{
  char error_buf[512];
  if (!inited)
  {
    perror("Ndb_ReloadHWInfo called on non-inited object");
    return -1;
  }

  FILE * cpuinfo = fopen("/proc/cpuinfo", "r");
  if (cpuinfo == nullptr)
  {
    perror("failed to open /proc/cpuinfo");
    return -1;
  }

  FileGuard g(cpuinfo); // close at end...

  char buf[1024];
  int curr_cpu = -1;
  Uint32 max_cpu_no = hwinfo->cpu_cnt_max - 1;
  for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
  {
    /**
     * No knowledge means that we treat all CPUs as equals in all
     * aspects, both CPU core, CPU socket and L3 cache.
     *
     * In this case it will be hard to perform any type of automatic
     * locking of threads to CPUs.
     */
    hwinfo->cpu_info[i].cpu_no = i;
    hwinfo->cpu_info[i].online = 0;
    hwinfo->cpu_info[i].l3_cache_id = 0;
  }
  Uint32 cpu_online_count = 0;
  Uint32 num_cpu_cores_per_socket = 0;
  while (fgets(buf, sizeof(buf), cpuinfo))
  {
    Uint32 val;
    char * p = nullptr;
    if (sscanf(buf, "processor : %u", &val) == 1)
    {
      if (val > max_cpu_no)
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU %u is outside max %u",
                 val,
                 max_cpu_no);
        perror(error_buf);
        return -1;
      }
      while (curr_cpu + 1 < (int)val)
      {
        curr_cpu++;
        hwinfo->cpu_info[curr_cpu].online = 0;
        hwinfo->cpu_info[curr_cpu].cpu_no = curr_cpu;
      }
      curr_cpu++;
      hwinfo->cpu_info[curr_cpu].cpu_no = curr_cpu;
      hwinfo->cpu_info[curr_cpu].online = 1;
      cpu_online_count++;
    }
    else if (sscanf(buf, "core id : %u", &val) == 1)
    {
      if (! (curr_cpu >= 0 && curr_cpu <= (int)max_cpu_no))
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU %u is outside max %u",
                 val,
                 max_cpu_no);
        perror(error_buf);
        return -1;
      }
      hwinfo->cpu_info[curr_cpu].core_id = val;
      DEBUG_HW((stderr,
                "CPU %u in core_id: %u\n",
                curr_cpu,
                val));
    }
    else if (sscanf(buf, "physical id : %u", &val) == 1)
    {
      if (! (curr_cpu >= 0 && curr_cpu <= (int)max_cpu_no))
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU %u is outside max %u",
                 val,
                 max_cpu_no);
        perror(error_buf);
        return -1;
      }
      hwinfo->cpu_info[curr_cpu].socket_id = val;
      hwinfo->cpu_info[curr_cpu].package_id = val;
    }
    else if (sscanf(buf, "cpu cores : %u", &val) == 1)
    {
      num_cpu_cores_per_socket = val;
    }
    else if ((p = strstr(buf, "model name")) != nullptr)
    {
      if (! (curr_cpu >= 0 && curr_cpu <= (int)max_cpu_no))
      {
        snprintf(error_buf,
                 sizeof(error_buf),
                 "CPU %u is outside max %u",
                 val,
                 max_cpu_no);
        perror(error_buf);
        return -1;
      }

      p = strstr(p, ": ");
      if (p)
      {
        p += 2;
        size_t sz = sizeof(hwinfo->cpu_model_name);
        strncpy(hwinfo->cpu_model_name, p, sz);
        hwinfo->cpu_model_name[sz - 1] = 0; // null terminate
        if (curr_cpu == 0)
        {
          DEBUG_HW((stderr,
                    "CPU model: %s\n",
                    p));
        }
      }
    }
  }
  Uint32 num_cpu_cores;
  if (num_cpu_cores_per_socket == 0)
  {
    /* Linux ARM needs information from other sources */
    hwinfo->cpu_cnt = cpu_online_count;
    DEBUG_HW((stderr,
              "Found %u CPUs online, no core info in /proc/cpuinfo\n",
              cpu_online_count));
    int res = get_core_siblings_info(hwinfo);
    if (res == -1)
    {
      return -1;
    }
    num_cpu_cores = (Uint32)res;
    hwinfo->num_cpu_cores = num_cpu_cores;
    res = get_physical_package_ids(hwinfo);
    if (res == -1)
    {
      return -1;
    }
  }
  else
  {
    DEBUG_HW((stderr,
              "Found %u CPU cores per CPU socket\n",
              num_cpu_cores_per_socket));
    Uint32 max_socket_id = 0;
    Uint32 max_core_id = 0;
    for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
    {
      if (hwinfo->cpu_info[i].online)
      {
        max_socket_id = MAX(max_socket_id, hwinfo->cpu_info[i].socket_id);
        max_core_id = MAX(max_core_id, hwinfo->cpu_info[i].core_id);
      }
    }
    Uint32 num_cpu_sockets = max_socket_id + 1;
    num_cpu_cores = num_cpu_cores_per_socket * num_cpu_sockets;
    hwinfo->num_cpu_sockets = num_cpu_sockets;
    hwinfo->num_cpu_cores = num_cpu_cores;
    for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
    {
      struct ndb_cpuinfo_data *cpuinfo = &hwinfo->cpu_info[i];
      if (cpuinfo->online)
      {
        cpuinfo->core_id += ((max_core_id + 1) * hwinfo->cpu_info[i].socket_id);
      }
    }
  }
  DEBUG_HW((stderr,
            "There are %u sockets\n",
            hwinfo->num_cpu_sockets));
  DEBUG_HW((stderr,
            "There are %u cores\n",
            hwinfo->num_cpu_cores));
 
  Uint32 num_cpu_per_core = hwinfo->cpu_cnt_max / num_cpu_cores;
  Uint32 cpu_cnt_max = num_cpu_per_core * num_cpu_cores;
  if (hwinfo->cpu_cnt_max != cpu_cnt_max)
  {
    hwinfo->is_mixed_cpu_thread_per_cpu_core = 1;
    DEBUG_HW((stderr,
          "The computer has a mix of number of CPU threads per CPU core\n"));
    hwinfo->num_cpu_per_core = 1;
  }
  else
  {
    DEBUG_HW((stderr,
              "We found %u CPUs per CPU core\n",
              num_cpu_per_core));
  }
  hwinfo->cpu_cnt = cpu_online_count;
  DEBUG_HW((stderr,
            "There are %u CPUs online before checking\n",
            cpu_online_count));

  get_cpu_throughput(hwinfo);

  Uint32 num_shared_l3_caches = 0;
  int res = get_l3_cache_info(hwinfo);
  if (res > 0)
  {
    num_shared_l3_caches = (Uint32)res;
    DEBUG_HW((stderr,
              "We found %u L3 cache regions\n",
              num_shared_l3_caches));
  }
  else if (res == (int)-1)
  {
    for (Uint32 i = 0; i < hwinfo->cpu_cnt_max; i++)
    {
      hwinfo->cpu_info[i].l3_cache_id = hwinfo->cpu_info[i].socket_id;
    }
    num_shared_l3_caches = hwinfo->num_cpu_sockets;
    DEBUG_HW((stderr,
              "We found 1 L3 cache region per socket\n"));
  }
  else
  {
    perror("Failed get_l3_cache_info");
    abort();
  }
  hwinfo->num_shared_l3_caches = num_shared_l3_caches;
  check_cpu_online(hwinfo);
  return get_meminfo(hwinfo);
}

#elif defined HAVE_SOLARIS_AFFINITY
/* Solaris HW Information */

/**
 * Solaris implementation
 */
static int NdbHW_Init_platform()
{
  return 0;
}

static void NdbHW_End_platform()
{
}

static int init_cpudata(struct ndb_hwinfo *)
{
  return 0;
}

static int Ndb_ReloadCPUData(struct ndb_hwinfo *)
{
  return 0;
}

static int init_hwinfo(struct ndb_hwinfo *)
{
  return 0;
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo * hwinfo)
{
  hwinfo->cpu_cnt_max = ncpu;
  hwinfo->cpu_cnt = ncpu;
  check_cpu_online(hwinfo);
  return 0;
}

#else

static int NdbHW_Init_platform()
{
  return -1;
}

static void NdbHW_End_platform()
{
}

static int init_hwinfo(struct ndb_hwinfo *)
{
  return -1;
}

static int init_cpudata(struct ndb_hwinfo *)
{
  return -1;
}

static int Ndb_ReloadHWInfo(struct ndb_hwinfo *)
{
  return -1;
}

static int Ndb_ReloadCPUData(struct ndb_hwinfo *)
{
  return -1;
}

#endif

#ifdef TEST_NDBHW
#include <NdbTap.hpp>

#define MAX_NUM_L3_CACHES 32
struct test_cpumap_data
{
  Uint32 num_l3_caches;
  Uint32 num_cpus_per_l3_cache;
  Uint32 num_cpus_in_l3_cache[MAX_NUM_L3_CACHES];
  Uint32 num_query_instances;
  Uint32 num_cpus_per_package;
  Uint32 num_p_cpus_per_core;
  Uint32 num_p_cpus_per_package;
  Uint32 num_e_cpus_per_package;
  Uint32 num_e_cpus_per_core;
  bool exact_core;
};

static void test_1(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 4;
  map->num_cpus_in_l3_cache[0] = 4;
  map->num_query_instances = 4;
  map->num_cpus_per_package = 4;
  map->exact_core = true;
  printf("Run test 1 with 1 L3 group with 4 CPUs, 4 Query\n");
}

static void test_2(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 16;
  map->num_query_instances = 14;
  map->num_cpus_per_package = 16;
  map->exact_core = true;
  printf("Run test 2 with 1 L3 group with 16 CPUs, 14 Query\n");
}

static void test_3(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 8;
  map->num_cpus_in_l3_cache[0] = 8;
  map->num_cpus_in_l3_cache[1] = 8;
  map->num_query_instances = 16;
  map->num_cpus_per_package = 8;
  map->exact_core = true;
  printf("Run test 3 with 2 L3 group with 8,8 CPUs, 16 Query\n");
}

static void test_4(struct test_cpumap_data *map)
{
  map->num_l3_caches = 4;
  map->num_cpus_per_l3_cache = 8;
  map->num_cpus_in_l3_cache[0] = 4;
  map->num_cpus_in_l3_cache[1] = 8;
  map->num_cpus_in_l3_cache[2] = 2;
  map->num_cpus_in_l3_cache[3] = 6;
  map->num_query_instances = 19;
  map->num_cpus_per_package = 32;
  printf("Run test 4 with 4 L3 group with 4,8,2,6 CPUs, 19 Query\n");
}

static void test_5(struct test_cpumap_data *map)
{
  map->num_l3_caches = 4;
  map->num_cpus_per_l3_cache = 8;
  map->num_cpus_in_l3_cache[0] = 4;
  map->num_cpus_in_l3_cache[1] = 8;
  map->num_cpus_in_l3_cache[2] = 2;
  map->num_cpus_in_l3_cache[3] = 4;
  map->num_cpus_per_package = 8;
  map->num_query_instances = 18;
  printf("Run test 5 with 4 L3 group with 4,8,2,4 CPUs, 18 Query\n");
}

static void test_6(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 30;
  map->num_cpus_in_l3_cache[0] = 30;
  map->num_cpus_in_l3_cache[1] = 30;
  map->num_query_instances = 50;
  map->num_cpus_per_package = 30;
  map->exact_core = true;
  printf("Run test 6 with 2 L3 group with 30,30 CPUs, 50 Query\n");
}

static void test_7(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 30;
  map->num_cpus_in_l3_cache[0] = 30;
  map->num_cpus_in_l3_cache[1] = 30;
  map->num_query_instances = 55;
  map->num_cpus_per_package = 60;
  map->exact_core = true;
  printf("Run test 7 with 2 L3 group with 30,30 CPUs, 55 Query\n");
}

static void test_8(struct test_cpumap_data *map)
{
  map->num_l3_caches = 3;
  map->num_cpus_per_l3_cache = 24;
  map->num_cpus_in_l3_cache[0] = 23;
  map->num_cpus_in_l3_cache[1] = 11;
  map->num_cpus_in_l3_cache[2] = 8;
  map->num_query_instances = 42;
  map->num_cpus_per_package = 24;
  printf("Run test 8 with 3 L3 group with 23,11,8 CPUs, 42 Query\n");
}

static void test_9(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 36;
  map->num_cpus_in_l3_cache[0] = 33;
  map->num_cpus_in_l3_cache[1] = 11;
  map->num_query_instances = 42;
  map->num_cpus_per_package = 36;
  printf("Run test 9 with 2 L3 group with 33,11 CPUs, 42 Query\n");
}

static void test_10(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 15;
  map->num_cpus_in_l3_cache[1] = 12;
  map->num_query_instances = 26;
  map->num_cpus_per_package = 16;
  printf("Run test 10 with 2 L3 group with 15,12 CPUs, 26 Query\n");
}

static void test_11(struct test_cpumap_data *map)
{
  map->num_l3_caches = 3;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 15;
  map->num_cpus_in_l3_cache[1] = 13;
  map->num_cpus_in_l3_cache[2] = 13;
  map->num_query_instances = 40;
  map->num_cpus_per_package = 16;
  printf("Run test 11 with 3 L3 group with 15,13,13 CPUs, 40 Query\n");
}

static void test_12(struct test_cpumap_data *map)
{
  map->num_l3_caches = 8;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 11;
  map->num_cpus_in_l3_cache[1] = 13;
  map->num_cpus_in_l3_cache[2] = 13;
  map->num_cpus_in_l3_cache[3] = 13;
  map->num_cpus_in_l3_cache[4] = 13;
  map->num_cpus_in_l3_cache[5] = 13;
  map->num_cpus_in_l3_cache[6] = 13;
  map->num_cpus_in_l3_cache[7] = 13;
  map->num_query_instances = 100;
  map->num_cpus_per_package = 32;
  printf("Run test 12 with 8 L3 group with 11,13,,,,13 CPUs, 100 Query\n");
}

static void test_13(struct test_cpumap_data *map)
{
  map->num_l3_caches = 8;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 16;
  map->num_cpus_in_l3_cache[1] = 16;
  map->num_cpus_in_l3_cache[2] = 16;
  map->num_cpus_in_l3_cache[3] = 16;
  map->num_cpus_in_l3_cache[4] = 16;
  map->num_cpus_in_l3_cache[5] = 16;
  map->num_cpus_in_l3_cache[6] = 8;
  map->num_cpus_in_l3_cache[7] = 8;
  map->num_query_instances = 110;
  map->num_cpus_per_package = 64;
  printf("Run test 13 with 8 L3 group with 16,,,16,8,8 CPUs, 110 Query\n");
}

static void test_14(struct test_cpumap_data *map)
{
  map->num_l3_caches = 16;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 1;
  map->num_cpus_in_l3_cache[1] = 1;
  map->num_cpus_in_l3_cache[2] = 1;
  map->num_cpus_in_l3_cache[3] = 1;
  map->num_cpus_in_l3_cache[4] = 1;
  map->num_cpus_in_l3_cache[5] = 1;
  map->num_cpus_in_l3_cache[6] = 1;
  map->num_cpus_in_l3_cache[7] = 1;
  map->num_cpus_in_l3_cache[8] = 1;
  map->num_cpus_in_l3_cache[9] = 1;
  map->num_cpus_in_l3_cache[10] = 1;
  map->num_cpus_in_l3_cache[11] = 1;
  map->num_cpus_in_l3_cache[12] = 1;
  map->num_cpus_in_l3_cache[13] = 1;
  map->num_cpus_in_l3_cache[14] = 1;
  map->num_cpus_in_l3_cache[15] = 1;
  map->num_query_instances = 16;
  map->num_cpus_per_package = 2;
  printf("Run test 14 with 16 L3 group with 1 CPU, 16 Query\n");
}

static void test_15(struct test_cpumap_data *map)
{
  map->num_l3_caches = 16;
  map->num_cpus_per_l3_cache = 16;
  map->num_cpus_in_l3_cache[0] = 1;
  map->num_cpus_in_l3_cache[1] = 2;
  map->num_cpus_in_l3_cache[2] = 4;
  map->num_cpus_in_l3_cache[3] = 1;
  map->num_cpus_in_l3_cache[4] = 3;
  map->num_cpus_in_l3_cache[5] = 1;
  map->num_cpus_in_l3_cache[6] = 1;
  map->num_cpus_in_l3_cache[7] = 16;
  map->num_cpus_in_l3_cache[8] = 1;
  map->num_cpus_in_l3_cache[9] = 8;
  map->num_cpus_in_l3_cache[10] = 1;
  map->num_cpus_in_l3_cache[11] = 3;
  map->num_cpus_in_l3_cache[12] = 1;
  map->num_cpus_in_l3_cache[13] = 2;
  map->num_cpus_in_l3_cache[14] = 4;
  map->num_cpus_in_l3_cache[15] = 2;
  map->num_query_instances = 50;
  map->num_cpus_per_package = 32;
  printf("Run test 15 with 16 L3 group with 0/1 CPU, 50 Query\n");
}

static void test_16(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 2;
  map->num_query_instances = 1;
  map->num_cpus_per_package = 2;
  map->exact_core = true;
  printf("Run test 16 with 1 L3 group with 2 CPU, 1 Query\n");
}

static void test_17(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 2;
  map->num_query_instances = 2;
  map->num_cpus_per_package = 1;
  printf("Run test 17 with 1 L3 group with 2 CPU, 2 Query\n");
}

static void test_18(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 2;
  map->num_query_instances = 1;
  map->num_cpus_per_package = 2;
  map->exact_core = true;
  printf("Run test 18 with 1 L3 group with 2 CPU, 1 Query\n");
}

static void test_19(struct test_cpumap_data *map)
{
  map->num_l3_caches = 1;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 2;
  map->num_query_instances = 2;
  map->num_cpus_per_package = 1;
  printf("Run test 19 with 1 L3 group with 2 CPU, 2 Query\n");
}

static void test_20(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 2;
  map->num_cpus_in_l3_cache[1] = 1;
  map->num_query_instances = 3;
  map->num_cpus_per_package = 2;
  printf("Run test 20 with 2 L3 group with 1 CPU, 3 Query\n");
}

static void test_21(struct test_cpumap_data *map)
{
  map->num_l3_caches = 3;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 1;
  map->num_cpus_in_l3_cache[1] = 1;
  map->num_cpus_in_l3_cache[2] = 1;
  map->num_query_instances = 3;
  map->num_cpus_per_package = 2;
  printf("Run test 21 with 3 L3 group with 1 CPU, 3 Query\n");
}

static void test_22(struct test_cpumap_data *map)
{
  map->num_l3_caches = 2;
  map->num_cpus_per_l3_cache = 2;
  map->num_cpus_in_l3_cache[0] = 1;
  map->num_cpus_in_l3_cache[1] = 1;
  map->num_query_instances = 2;
  map->num_cpus_per_package = 2;
  printf("Run test 22 with 2 L3 group with 1 CPU, 2 Query\n");
}

static void
create_hwinfo_test_cpu_map(struct test_cpumap_data *map)
{
  Uint32 num_cpus = map->num_cpus_per_l3_cache * map->num_l3_caches;
  g_ndb_hwinfo = (struct ndb_hwinfo*)malloc(sizeof(struct ndb_hwinfo));
  void *ptr = malloc(sizeof(struct ndb_cpuinfo_data) * num_cpus);
  g_ndb_hwinfo->cpu_info = (struct ndb_cpuinfo_data*)ptr;
  g_ndb_hwinfo->cpu_cnt_max = num_cpus;
  ncpu = num_cpus;
  g_ndb_hwinfo->cpu_cnt = num_cpus;
  g_ndb_hwinfo->num_shared_l3_caches = map->num_l3_caches;
  g_ndb_hwinfo->is_cpuinfo_available = 1;
  Uint32 cpu_id = 0;
  struct ndb_hwinfo *hwinfo = g_ndb_hwinfo;
  Uint32 core_id = 0;
  Uint32 package_id = 0;
  Uint32 core_cpu_count = 0;
  Uint32 num_cpus_per_core = map->num_p_cpus_per_core;
  ncpu = 0;
  for (Uint32 l3_cache_id = 0; l3_cache_id < map->num_l3_caches; l3_cache_id++)
  {
    require(map->num_cpus_per_l3_cache >=
            map->num_cpus_in_l3_cache[l3_cache_id]);
    for (Uint32 i = 0; i < map->num_cpus_per_l3_cache; i++)
    {
      if (i < map->num_cpus_in_l3_cache[l3_cache_id])
      {
        /* Online CPU */
        hwinfo->cpu_info[cpu_id].l3_cache_id = l3_cache_id;
        hwinfo->cpu_info[cpu_id].cpu_no = cpu_id;
        hwinfo->cpu_info[cpu_id].core_id = core_id;
        hwinfo->cpu_info[cpu_id].socket_id = package_id;
        hwinfo->cpu_info[cpu_id].package_id = package_id;
        hwinfo->cpu_info[cpu_id].online = 1;
      }
      else
      {
        hwinfo->cpu_info[cpu_id].l3_cache_id = l3_cache_id;
        hwinfo->cpu_info[cpu_id].socket_id = package_id;
        hwinfo->cpu_info[cpu_id].package_id = package_id;
        hwinfo->cpu_info[cpu_id].online = 0;
        hwinfo->cpu_info[cpu_id].core_id = core_id;
      }
      ncpu++;
      cpu_id++;
      core_cpu_count++;
      if (core_cpu_count == num_cpus_per_core)
      {
        core_id++;
        core_cpu_count = 0;
      }
      if (cpu_id == map->num_p_cpus_per_package)
      {
        num_cpus_per_core = map->num_e_cpus_per_core;
        core_cpu_count = 0;
      }
      if (cpu_id == map->num_cpus_per_package)
      {
        package_id++;
        core_id = 0;
        num_cpus_per_core = map->num_p_cpus_per_core;
        core_cpu_count = 0;
      }
    }
  }
  create_l3_cache_list(hwinfo);
}

static void
cleanup_test()
{
  struct ndb_hwinfo *hwinfo = g_ndb_hwinfo;
  for (Uint32 i = 0; i < hwinfo->num_virt_l3_caches; i++)
  {
    printf("Virtual L3 Group[%u] = %u\n",
           i, g_num_virt_l3_cpus[i]);
    Uint32 next_cpu = g_first_virt_l3_cache[i];
    do
    {
      printf("    CPU %u, core: %u, l3_cache_id: %u\n",
             next_cpu,
             hwinfo->cpu_info[next_cpu].core_id,
             hwinfo->cpu_info[next_cpu].l3_cache_id);
      next_cpu = hwinfo->cpu_info[next_cpu].next_virt_l3_cpu_map;
    } while (next_cpu != RNIL);
  }
  printf("CPU list created for CPU lock assignment\n");
  Uint32 next_cpu = hwinfo->first_cpu_map;
  do
  {
    printf("    CPU %u, core: %u, l3_cache_id: %u\n",
           next_cpu,
           hwinfo->cpu_info[next_cpu].core_id,
           hwinfo->cpu_info[next_cpu].l3_cache_id);
    next_cpu = hwinfo->cpu_info[next_cpu].next_cpu_map;
  } while (next_cpu != RNIL);
  ncpu = 0;
  free((void*)g_first_l3_cache);
  free((void*)g_first_virt_l3_cache);
  free((void*)g_num_l3_cpus);
  free((void*)g_num_l3_cpus_online);
  free((void*)g_num_virt_l3_cpus);
  free((void*)g_ndb_hwinfo->cpu_info);
  free((void*)g_ndb_hwinfo);
  g_ndb_hwinfo = nullptr;
  g_first_l3_cache = nullptr;
  g_first_virt_l3_cache = nullptr;
  g_num_l3_cpus = nullptr;
  g_num_virt_l3_cpus = nullptr;
  g_num_l3_cpus_online = nullptr;
}

static void
test_create(struct test_cpumap_data *map, Uint32 test_case)
{
  /* Set default values */
  map->exact_core = false;
  map->num_p_cpus_per_core = 2;
  map->num_e_cpus_per_core = 1;
  map->num_p_cpus_per_package = 0;
  map->num_e_cpus_per_package = 0;
  switch (test_case)
  {
    case 0:
    {
      test_1(map);
      break;
    }
    case 1:
    {
      test_2(map);
      break;
    }
    case 2:
    {
      test_3(map);
      break;
    }
    case 3:
    {
      test_4(map);
      break;
    }
    case 4:
    {
      test_5(map);
      break;
    }
    case 5:
    {
      test_6(map);
      break;
    }
    case 6:
    {
      test_7(map);
      break;
    }
    case 7:
    {
      test_8(map);
      break;
    }
    case 8:
    {
      test_9(map);
      break;
    }
    case 9:
    {
      test_10(map);
      break;
    }
    case 10:
    {
      test_11(map);
      break;
    }
    case 11:
    {
      test_12(map);
      break;
    }
    case 12:
    {
      test_13(map);
      break;
    }
    case 13:
    {
      test_14(map);
      break;
    }
    case 14:
    {
      test_15(map);
      break;
    }
    case 15:
    {
      test_16(map);
      break;
    }
    case 16:
    {
      test_17(map);
      break;
    }
    case 17:
    {
      test_18(map);
      break;
    }
    case 18:
    {
      test_19(map);
      break;
    }
    case 19:
    {
      test_20(map);
      break;
    }
    case 20:
    {
      test_21(map);
      break;
    }
    case 21:
    {
      test_22(map);
      break;
    }
    default:
    {
      require(false);
      break;
    }
  }
  if (map->num_p_cpus_per_package == 0)
    map->num_p_cpus_per_package = map->num_cpus_per_package;
  require(map->num_cpus_per_package ==
          (map->num_p_cpus_per_package +
           map->num_e_cpus_per_package));
  return;
}

#define NUM_TESTS 22
static void
test_create_cpumap()
{
  Uint32 expected_res[NUM_TESTS];
  expected_res[0] = 1;
  expected_res[1] = 1;
  expected_res[2] = 2;
  expected_res[3] = 5;
  expected_res[4] = 2;
  expected_res[5] = 2;
  expected_res[6] = 3;
  expected_res[7] = 5;
  expected_res[8] = 3;
  expected_res[9] = 2;
  expected_res[10] = 3;
  expected_res[11] = 12;
  expected_res[12] = 12;
  expected_res[13] = 1;
  expected_res[14] = 6;
  expected_res[15] = 1;
  expected_res[16] = 1;
  expected_res[17] = 1;
  expected_res[18] = 1;
  expected_res[19] = 1;
  expected_res[20] = 1;
  expected_res[21] = 1;
  struct test_cpumap_data test_map;
  for (Uint32 i = 0; i < NUM_TESTS; i++)
  {
    printf("Start test %u\n", i + 1);
    test_create(&test_map, i);
    printf("Create HW info for test %u\n", i + 1);
    create_hwinfo_test_cpu_map(&test_map);
    printf("Create CPUMap for test %u\n", i + 1);
    Uint32 num_rr_groups =
      Ndb_CreateCPUMap(test_map.num_query_instances, 1024);
    for (Uint32 id = 0; id < g_ndb_hwinfo->cpu_cnt_max; id++)
    {
      Uint32 cpu_ids[MAX_USED_NUM_CPUS];
      Uint32 num_cpus;
      if (g_ndb_hwinfo->cpu_info[id].online)
      {
        Ndb_GetCoreCPUIds(id, &cpu_ids[0], num_cpus);
        printf("Ndb_GetCoreCPUIds: id: %u, num_cpus: %u\n",
               id,
               num_cpus);
        if (test_map.exact_core)
        {
          OK(num_cpus == (test_map.num_p_cpus_per_core));
        }
        else
        {
          OK(num_cpus <= (test_map.num_p_cpus_per_core));
        }
      }
    }
    cleanup_test();
    printf("Test %u num_rr_groups: %u, expected: %u\n",
           i + 1,
           num_rr_groups,
           expected_res[i]);
    OK(num_rr_groups == expected_res[i]);
  }
  printf("test_create_cpumap passed\n");
}

void
printdata(const struct ndb_hwinfo* data, Uint32 cpu)
{
  uintmax_t sum_sys = 0;

  for (Uint32 i = 0; i < data->cpu_cnt; i++)
  {
    sum_sys += data->cpu_data[i].cs_sys_us;
    sum_sys += data->cpu_data[i].cs_irq_us;
    sum_sys += data->cpu_data[i].cs_sirq_us;
    sum_sys += data->cpu_data[i].cs_guest_us;
    sum_sys += data->cpu_data[i].cs_guest_nice_us;
  }

  uintmax_t elapsed = 0;
  elapsed += data->cpu_data[cpu].cs_user_us;
  elapsed += data->cpu_data[cpu].cs_idle_us;
  elapsed += data->cpu_data[cpu].cs_nice_us;
  elapsed += data->cpu_data[cpu].cs_sys_us;
  elapsed += data->cpu_data[cpu].cs_iowait_us;
  elapsed += data->cpu_data[cpu].cs_irq_us;
  elapsed += data->cpu_data[cpu].cs_steal_us;
  elapsed += data->cpu_data[cpu].cs_sirq_us;
  elapsed += data->cpu_data[cpu].cs_guest_us;
  elapsed += data->cpu_data[cpu].cs_guest_nice_us;

  uintmax_t cpu_sys = 0;
  cpu_sys += data->cpu_data[cpu].cs_sys_us;
  cpu_sys += data->cpu_data[cpu].cs_irq_us;
  cpu_sys += data->cpu_data[cpu].cs_sirq_us;
  cpu_sys += data->cpu_data[cpu].cs_guest_us;
  cpu_sys += data->cpu_data[cpu].cs_guest_nice_us;
  cpu_sys += data->cpu_data[cpu].cs_steal_us;

  printf("Cpu %u time: %juus sys: %ju%% All cpu sys: %juus\n",
         cpu,
         elapsed,
         elapsed ? (100 * cpu_sys) / elapsed : 0,
         sum_sys);
}

TAPTEST(NdbCPU)
{
  printf("Start NdbHW test\n");
  test_create_cpumap();
  int res = NdbHW_Init();
  if (res < 0)
  {
    OK(1);
    return 0;
  }

  ndb_init();
  long sysconf_ncpu_conf = 0;
#ifdef _SC_NPROCESSORS_CONF
  {
    long tmp = sysconf(_SC_NPROCESSORS_CONF);
    if (tmp < 0)
    {
      perror("sysconf(_SC_NPROCESSORS_CONF) returned error");
      abort();
    }
    else
    {
      sysconf_ncpu_conf = (Uint32) tmp;
    }
  }
#else
  sysconf_ncpu_conf = std::thread::hardware_concurrency();
#endif
  printf("sysconf(_SC_NPROCESSORS_CONF) => %lu\n", sysconf_ncpu_conf);

#ifdef _SC_NPROCESSORS_ONLN
  long sysconf_ncpu_online = 0;
  sysconf_ncpu_online = sysconf(_SC_NPROCESSORS_ONLN);
  printf("sysconf(_SC_NPROCESSORS_ONLN) => %lu\n", sysconf_ncpu_online);
#endif

  struct ndb_hwinfo *info = g_ndb_hwinfo;
  /**
   * Test of CPU info
   */
  OK(info != nullptr);
  if (sysconf_ncpu_conf)
  {
    OK(sysconf_ncpu_conf == (long)info->cpu_cnt);
  }
  Ndb_FreeHWInfo();
  NdbHW_End();
  ndb_end(0);
  return 1; // OK
}
#endif
