/* Copyright (c) 2009, 2023, Oracle and/or its affiliates.
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
#include "util/require.h"
#include <ndb_global.h>

#include <algorithm>

#include <NdbEnv.h>
#include <NdbConfig.h>
#include <NdbSleep.h>
#include <portlib/NdbDir.hpp>
#include <NdbAutoPtr.hpp>
#include <portlib/NdbNuma.h>

#include "vm/SimBlockList.hpp"
#include "vm/WatchDog.hpp"
#include "vm/ThreadConfig.hpp"
#include "vm/Configuration.hpp"

#include "ndb_stacktrace.h"
#include "ndbd.hpp"
#include "main.hpp"

#include <TransporterRegistry.hpp>

#include <ConfigRetriever.hpp>
#include <LogLevel.hpp>

#if defined NDB_SOLARIS
#include <sys/processor.h>
#endif

#include <NdbGetRUsage.h>
#include <ndb_openssl_evp.h>
#include <EventLogger.hpp>
#include <LogBuffer.hpp>
#include <OutputStream.hpp>

#define JAM_FILE_ID 484


static void
systemInfo(const Configuration & config, const LogLevel & logLevel)
{
#ifdef _WIN32
  int processors = 0;
  int speed;
  SYSTEM_INFO sinfo;
  GetSystemInfo(&sinfo);
  processors = sinfo.dwNumberOfProcessors;
  HKEY hKey;
  if(ERROR_SUCCESS==RegOpenKeyEx
     (HKEY_LOCAL_MACHINE,
      TEXT("HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0"),
      0, KEY_READ, &hKey)) {
    DWORD dwMHz;
    DWORD cbData = sizeof(dwMHz);
    if(ERROR_SUCCESS==RegQueryValueEx(hKey,
				      "~MHz", 0, 0, (LPBYTE)&dwMHz, &cbData)) {
      speed = int(dwMHz);
    }
    RegCloseKey(hKey);
  }
#elif defined NDB_SOLARIS
  // Search for at max 16 processors among the first 256 processor ids
  processor_info_t pinfo;

  memset(&pinfo, 0, sizeof(pinfo));
  int pid = 0;
  while(processors < 16 && pid < 256){
    if(!processor_info(pid++, &pinfo))
      processors++;
  }
  speed = pinfo.pi_clock;
#endif

  if(logLevel.getLogLevel(LogLevel::llStartUp) > 0){
    g_eventLogger->info("RonDB -- DB node %d", globalData.ownId);
    g_eventLogger->info("%s --", NDB_VERSION_STRING);
#ifdef NDB_SOLARIS
    g_eventLogger->info("NDB is running on a machine with %d processor(s) at %d MHz",
                        processor, speed);
#endif
  }
  if(logLevel.getLogLevel(LogLevel::llStartUp) > 3){
    Uint32 t = config.timeBetweenWatchDogCheck();
    g_eventLogger->info("WatchDog timer is set to %d ms", t);
  }
}

/**
 * We currently allocate the following large chunks of memory:
 * -----------------------------------------------------------
 *
 * RG_DATAMEM:
 * This is a resource which one sets the min == max. This means that
 * we cannot overallocate this resource. The size of the resource
 * is based on the sum of the configuration variables DataMemory
 * and IndexMemory. It's used for main memory tuples, indexes and
 * hash indexes. We add an extra 8 32kB pages for safety reasons
 * if IndexMemory is set.
 *
 * RG_FILE_BUFFERS:
 * This is a resource used by the REDO log handler in DBLQH. It is
 * also a resource we cannot overallocate. The size of it is based
 * on the multiplication of the config variables NoOfLogFileParts
 * and RedoBuffer. In addition we add a constant 1 MByte per each
 * log file part to handle some extra outstanding requests.
 *
 * RG_JOB_BUFFERS:
 * This is a resource used to allocate job buffers by multithreaded
 * NDB scheduler to handle various job buffers and alike. It has
 * a complicated algorithm to calculate its size. It allocates a
 * bit more than 2 MByte per thread and it also allocates a 1 MByte
 * buffer in both directions for all threads that can communicate.
 * For large configurations this becomes a fairly large memory
 * that can consume up to a number of GBytes of memory. It is
 * also a resource that cannot be overallocated.
 *
 * RG_TRANSPORTER_BUFFERS:
 * This is a resource used for send buffers in ndbmtd. It is set to
 * a size of the sum of TotalSendBufferMemory and ExtraSendBufferMemory.
 * It is a resource that can be overallocated by 25%.
 * TotalSendBufferMemory is by default set to 0. In this the this
 * variable is calculated by summing the send buffer memory per node.
 * The default value per send buffer per node is 2 MByte. So this
 * means that in a system with 4 data nodes and 8 client nodes the
 * data nodes will have 11 * 2 MByte of total memory. The extra
 * memory is by default 0. However for ndbmtd we also add more memory
 * in the extra part. We add 2 MBytes per thread used in the node.
 * So with 4 LDM threads, 2 TC threads, 1 main thread, 1 rep thread,
 * and 2 receive threads then we have 10 threads and allocate another
 * extra 20 MBytes. The user can never set this below 16MByte +
 * 2 MByte per thread + 256 kByte per node. So default setting is
 * 2MByte * (#nodes + #threads) and we can oversubscribe by 25% by
 * using the SharedGlobalMemory.
 *
 * RG_DISK_PAGE_BUFFER:
 * This is a resource that is used for the disk page buffer. It cannot
 * be overallocated. Its size is calculated based on the config variable
 * DiskPageBufferMemory.
 *
 * RG_SCHEMA_TRANS_MEMORY:
 * This is a resource that is set to a minimum of 2 MByte. It can be
 * overallocated at any size as long as there is still memory
 * remaining.
 *
 * RG_TRANSACTION_MEMORY:
 * This is a resource that is either set to zero size but can be overallocated
 * without limit. If a log file group is allocated based on the config, then
 * the size of the UNDO log buffer is used to set the size of this resource.
 * This resource is only used to allocate the UNDO log buffer of an UNDO log
 * file group and there can only be one such group. It is using overallocating
 * if this happens through an SQL command.
 *
 * RG_QUERY_MEMORY:
 * Like transaction memory, query memory may be overallocated. Unlike
 * transaction memory, query memory may not use the last free 10% of shared
 * global memory.  This is controlled by setting the reserved memory to zero.
 * This indicates to memory manager that the resource is low priority and
 * should not be allowed to starve out other higher priority resource.
 *
 * Dbspj is the user of query memory serving join queries and "only" read data.
 * A bad join query could easily consume a lot of memory.
 *
 * Dbtc, which uses transaction memory, on the other hand also serves writes,
 * and typically memory consumption per request are more limited.
 *
 * In situations there there are small amount of free memory left one want to
 * Dbtc to be prioritized over Dbspj.
 *
 * RG_REPLICATION_MEMORY:
 * This memory is used by SUMA to store records of changes to be sent out
 * to the clients subscribing to replication events in NDB Cluster.
 * Similar to the RG_QUERY_MEMORY it is a low priority resource, but it can
 * grow as long as there is no memory shortage. It will never decrease
 * below the configured memory size (configure parameter is
 * MaxBufferedEpochBytes).
 *
 * RG_SCHEMA_MEMORY:
 * This memory is used to handle memory resources connected to schema
 * objects such as tables, fragments and so forth. It can grow substantially
 * beyond its configured value since it is a prioritized resource.
 *
 * Overallocating and total memory
 * -------------------------------
 * The total memory allocated by the global memory manager is the sum of the
 * sizes of the above resources. On top of this one also adds the global
 * shared memory resource. The size of this is set to the config variable
 * SharedGlobalMemory. The global shared memory resource is the resource used
 * when we're overallocating as is currently possible for the UNDO log
 * memory and also for schema transaction memory. GlobalSharedMemory cannot
 * be set lower than 128 MByte.
 */
static int
init_global_memory_manager(EmulatorData &ed, Uint32 *watchCounter)
{
  const ndb_mgm_configuration_iterator * p =
    ed.theConfiguration->getOwnConfigIterator();
  if (p == nullptr)
  {
    g_eventLogger->alert("Failed to get node config iterator, "
                         "exiting.");
    return -1;
  }

  Uint32 numa = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_NUMA, &numa);
  if (numa == 1)
  {
    int res = NdbNuma_setInterleaved();
    g_eventLogger->info("numa_set_interleave_mask(numa_all_nodes) : %s",
                        res == 0 ? "OK" : "no numa support");
  }

  Uint64 shared_mem = globalData.theSharedGlobalMemory;
  Uint32 shared_pages = Uint32(shared_mem /= GLOBAL_PAGE_SIZE);

  g_eventLogger->info("SharedGlobalMemory set to %u MB", shared_pages/32);
  Uint64 dataMem = globalData.theDataMemory;
  Uint32 tupmem= (Uint32)(dataMem / 32768);

  Uint32 lqhInstances = globalData.ndbMtLqhWorkers;

  if (tupmem)
  {
    Resource_limit rl;
    rl.m_min = tupmem;
    rl.m_max = tupmem;
    rl.m_max_high_prio = tupmem;
    rl.m_resource_id = RG_DATAMEM;
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);

    Uint32 free_pct = 5;
    ndb_mgm_get_int_parameter(p, CFG_DB_FREE_PCT, &free_pct);
    ed.m_mem_manager->init_resource_spare(RG_DATAMEM, free_pct);
  }
  else
  {
    g_eventLogger->alert("No data memory, exiting.");
    return -1;
  }
  g_eventLogger->info("DataMemory set to %u MB", tupmem/32);

  Uint32 logParts = NDB_DEFAULT_LOG_PARTS;
  ndb_mgm_get_int_parameter(p, CFG_DB_NO_REDOLOG_PARTS, &logParts);

  Uint32 maxopen = logParts * 4; // 4 redo parts, max 4 files per part
  Uint32 filebuffer = NDB_FILE_BUFFER_SIZE;
  Uint32 filepages = (filebuffer / GLOBAL_PAGE_SIZE) * maxopen;
  globalData.ndbLogParts = logParts;

  {
    /**
     * RedoBuffer
     */
    Uint32 redomem = globalData.theRedoBuffer;
    if (redomem)
    {
      redomem /= GLOBAL_PAGE_SIZE;
      Uint32 tmp = redomem & 15;
      if (tmp != 0)
      {
        redomem += (16 - tmp);
      }

      filepages += logParts * redomem; // Add to RG_FILE_BUFFERS
    }
  }

  Resource_limit rl;
  rl.m_min = filepages;
  rl.m_max = filepages;
  rl.m_max_high_prio = filepages;
  rl.m_resource_id = RG_FILE_BUFFERS;
  // Cannot use last piece of memory
  rl.m_prio_memory = Resource_limit::HIGH_PRIO_MEMORY;
  ed.m_mem_manager->set_resource_limit(rl);
  g_eventLogger->info("RedoLogBuffer uses %u MB", filepages/32);

  Uint32 jbpages = compute_jb_pages(&ed);
  if (jbpages)
  {
    require(globalData.isNdbMt);
    Resource_limit rl;
    shared_pages += (jbpages / 2);
    rl.m_min = jbpages / 2;
    rl.m_max = jbpages;
    rl.m_max_high_prio = jbpages;
    rl.m_resource_id = RG_JOBBUFFER;
    // Can use last piece of memory
    rl.m_prio_memory = Resource_limit::ULTRA_HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
    g_eventLogger->info("Job buffers use up to %u MB", jbpages/32);
  }
  else
  {
    g_eventLogger->alert("No job buffer memory, exiting.");
    return -1;
  }

  Uint32 sbpages = 0;
  require(globalData.isNdbMt);
  {
    /**
     * This path is normally always taken for ndbmtd as the transporter
     * registry defined in mt.cpp is hard coded to set this to false.
     * For ndbd it is hard coded similarly to be set to true in
     * TransporterCallback.cpp. So for ndbd this code isn't executed.
     */
    Uint64 mem = Configuration::get_send_buffer(p);
    sbpages = Uint32((mem + GLOBAL_PAGE_SIZE - 1) / GLOBAL_PAGE_SIZE);
    shared_pages += (sbpages / 2);

    Resource_limit rl;
    rl.m_min = sbpages / 2;
    /**
     * allow over allocation (from SharedGlobalMemory) of up to 50% of
     *   totally allocated SendBuffer, at most 25% of SharedGlobalMemory.
     */
    Uint32 max_use_of_shared_pages = shared_pages / 4;
    const Uint32 sb_max_shared_pages =
      50 * std::min(sbpages, max_use_of_shared_pages) / 100;
    require(sbpages + sb_max_shared_pages > 0);
    rl.m_max = sbpages + sb_max_shared_pages;
    rl.m_max_high_prio = sbpages + sb_max_shared_pages;
    rl.m_resource_id = RG_TRANSPORTER_BUFFERS;
    // Can use last piece of memory
    rl.m_prio_memory = Resource_limit::ULTRA_HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
    g_eventLogger->info("Send buffers use up to %u MB, can overallocate"
                        " %u MB more using SharedGlobalMemory.",
                        sbpages / 32,
                        sb_max_shared_pages / 32);
  }

  Uint32 pgman_pages = 0;
  {
    /**
     * Disk page buffer memory
     */
    Uint32 recoverInstances = globalData.ndbMtRecoverThreads +
                              globalData.ndbMtQueryThreads;
    Uint64 page_buffer = globalData.theDiskPageBufferMemory;

    Uint32 pages = 0;
    pages += Uint32(page_buffer / GLOBAL_PAGE_SIZE); // in pages
    pages += LCP_RESTORE_BUFFER * lqhInstances;
    pages += LCP_RESTORE_BUFFER * recoverInstances;

    pgman_pages += pages;
    pgman_pages += 64;

    Resource_limit rl;
    rl.m_min = pgman_pages;
    require(pgman_pages > 0);
    rl.m_max = pgman_pages;
    rl.m_max_high_prio = pgman_pages;
    rl.m_resource_id = RG_DISK_PAGE_BUFFER;  // Add to RG_DISK_PAGE_BUFFER
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  Uint32 pgman_mbytes = pgman_pages / 32;
  g_eventLogger->info("DiskPageBuffer uses %u MB", pgman_mbytes);

  Uint32 stpages = 128;
  {
    Resource_limit rl;
    rl.m_min = stpages;
    rl.m_max = Resource_limit::HIGHEST_LIMIT;
    rl.m_max_high_prio = Resource_limit::HIGHEST_LIMIT;
    rl.m_resource_id = RG_SCHEMA_TRANS_MEMORY;
    /**
     * If we are in a tight memory situation it seems like a good idea to
     * not allow creation of new schema objects.
     */
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::LOW_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  g_eventLogger->info("SchemaTransactionMemory uses 4 MB");

  Uint64 TransactionMemory = globalData.theTransactionMemory;
  Uint32 transmem = Uint32(TransactionMemory / Uint64(32768));
  g_eventLogger->info("TransactionMemory set to %u MB", transmem/32);

  if (globalData.theUndoBuffer != 0)
  {
    Uint64 undo_buffer_pages = globalData.theUndoBuffer /
                               Uint64(GLOBAL_PAGE_SIZE);
    g_eventLogger->info("Adding %llu MBytes to TransactionMemory"
                        " for Undo Buffer",
                        globalData.theUndoBuffer / MBYTE64);
    transmem += undo_buffer_pages;
  }
  {
    Resource_limit rl;
    Uint32 shared_pages_part = shared_pages / 2;
    rl.m_min = transmem;
    rl.m_max = Resource_limit::HIGHEST_LIMIT;
    rl.m_max_high_prio = transmem + shared_pages_part;
    rl.m_resource_id = RG_TRANSACTION_MEMORY;
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
    if (globalData.theUndoBuffer == 0)
    {
      g_eventLogger->info("No Undo log buffer used, will be allocated from"
                          " TransactionMemory if later defined by command");
    }
    g_eventLogger->info("Total TransactionMemory size is %u MBytes",
                        transmem/32);
    g_eventLogger->info("TransactionMemory can expand and use"
                        " SharedGlobalMemory if required");
  }
  {
    Resource_limit rl;
    /*
     * Setting LOW_PRIO_MEMORY makes QUERY_MEMORY a low priority resource
     * group which can not use the last 10% of shared global page memory
     * (when the ultra high part has been removed).
     *
     * For example TRANSACTION_MEMORY will have access to those last
     * percent of global shared global page memory.
     */
    rl.m_min = 0;
    rl.m_max = Resource_limit::HIGHEST_LIMIT;
    rl.m_max_high_prio = Resource_limit::HIGHEST_LIMIT;
    rl.m_resource_id = RG_QUERY_MEMORY;
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::LOW_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  g_eventLogger->info("QueryMemory can use memory from SharedGlobalMemory"
                      " until 90%% used");

  /**
   * We add 32 MByte for replication memory, but this memory will get memory
   * from SharedGlobalMemory and the resources using this memory up to 90%
   * of the memory limit. So no concern that it is small for now.
   * Mostly not set to 0 for small installations that might not take height
   * for this memory.
   */
  Uint64 ReplicationMemory = globalData.theReplicationMemory;
  Uint32 replication_memory = Uint32(ReplicationMemory / Uint64(32768));
  Uint32 rep_mb = replication_memory / 32;
  g_eventLogger->info("Adding %u MByte for replication memory", rep_mb);
  {
    Resource_limit rl;
    Uint32 shared_pages_part = (shared_pages / 10) * 3;
    rl.m_max = replication_memory + shared_pages_part;
    rl.m_max_high_prio = replication_memory + shared_pages_part;
    rl.m_resource_id = RG_REPLICATION_MEMORY;
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  g_eventLogger->info("MaxBufferedEpochBytes can use memory from"
                      " SharedGlobalMemory until 90%% used");

  Uint64 SchemaMemory = globalData.theSchemaMemory;
  Uint32 schema_memory = Uint32(SchemaMemory / Uint64(32768));
  {
    Resource_limit rl;
    Uint32 shared_pages_part = (shared_pages / 10) * 2;
    rl.m_min = schema_memory;
    rl.m_max = schema_memory + shared_pages_part;
    rl.m_max_high_prio = schema_memory + shared_pages_part;
    rl.m_resource_id = RG_SCHEMA_MEMORY;
    // Cannot use last piece of memory
    rl.m_prio_memory = Resource_limit::LOW_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  g_eventLogger->info("Adding %u MByte for schema memory", schema_memory/32);
  g_eventLogger->info("SchemaMemory can expand and use"
                      " SharedGlobalMemory if required");

  Uint64 BackupSchemaMemory = globalData.theBackupSchemaMemory;
  Uint32 backup_schema_memory = Uint32(BackupSchemaMemory / Uint64(32768));
  {
    Resource_limit rl;
    rl.m_min = backup_schema_memory;
    rl.m_max = Resource_limit::HIGHEST_LIMIT;
    rl.m_max_high_prio = Resource_limit::HIGHEST_LIMIT;
    rl.m_resource_id = RG_BACKUP_SCHEMA_MEMORY;
    // Can use last piece of memory
    rl.m_prio_memory = Resource_limit::ULTRA_HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }
  g_eventLogger->info("Adding %u MByte for backup schema memory",
                      backup_schema_memory/32);
  g_eventLogger->info("BackupSchemaMemory can expand and use"
                      " SharedGlobalMemory if required");
  {
    Resource_limit rl;
    rl.m_min = 0;
    rl.m_max = Resource_limit::HIGHEST_LIMIT;
    rl.m_max_high_prio = Resource_limit::HIGHEST_LIMIT;
    rl.m_resource_id = RG_DISK_RECORDS;
    // Can use last piece of memory
    rl.m_prio_memory = Resource_limit::ULTRA_HIGH_PRIO_MEMORY;
    ed.m_mem_manager->set_resource_limit(rl);
  }

  Uint32 sum = shared_pages + tupmem + filepages + jbpages + sbpages +
    pgman_pages + stpages + transmem + replication_memory + schema_memory +
    backup_schema_memory;

  /**
   * We allocate a bit of extra pages to handle map pages in the NDB memory
   * manager. We need 2 extra pages per 8 GByte of memory added and one more
   * page per chunk. We add the first chunk already here.
   */
  Uint32 extra_chunk_pages = (2 * (sum / (32768 * 8))) + 3;
  sum += extra_chunk_pages;

  g_eventLogger->info("Total sum of all pages is %u", sum);
  if (!ed.m_mem_manager->init(watchCounter, sum, false))
  {
    g_eventLogger->alert("Malloc (%lld bytes) for %s and others failed, exiting",
                         Uint64(shared_mem + tupmem) * GLOBAL_PAGE_SIZE,
                         "DataMemory");
    return -1;
  }

  Uint32 late_alloc = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_LATE_ALLOC,
                            &late_alloc);

  Uint32 memlock = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_MEMLOCK, &memlock);

  if (late_alloc)
  {
    /**
     * Only map these groups that are required for ndb to even "start"
     */
    Uint32 rg[] = { RG_JOBBUFFER, RG_FILE_BUFFERS, RG_TRANSPORTER_BUFFERS, 0 };
    ed.m_mem_manager->map(watchCounter, memlock, rg);
  }
  else
  {
    ed.m_mem_manager->map(watchCounter, memlock); // Map all
  }
  Uint32 ultra_prio_free_limit = (jbpages / 2) + (sbpages / 2);
  ed.m_mem_manager->set_prio_free_limits(ultra_prio_free_limit);
  ed.m_mem_manager->lock();
  ed.m_mem_manager->check();
  ed.m_mem_manager->unlock();
  ed.m_mem_manager->init_memory_pools();
  return 0;                     // Success
}


static int
get_multithreaded_config(EmulatorData& ed)
{
  // multithreaded is compiled in ndbd/ndbmtd for now
  if (!globalData.isNdbMt)
  {
    g_eventLogger->info("NDBMT: non-mt");
    return 0;
  }

  THRConfig & conf = ed.theConfiguration->m_thr_config;
  Uint32 threadcount = conf.getThreadCount();
  g_eventLogger->info("NDBMT: MaxNoOfExecutionThreads=%u", threadcount);

  if (!globalData.isNdbMtLqh)
    return 0;

  g_eventLogger->info("NDBMT: ldm_workers=%u ldm_threads=%u"
                      " query_workers=%u query_threads=%u\n"
                      " tc_threads=%u tc_workers=%u"
                      " send=%u receive=%u main_threads=%u",
                      globalData.ndbMtLqhWorkers, globalData.ndbMtLqhThreads,
                      globalData.ndbMtQueryWorkers, globalData.ndbMtQueryThreads,
                      globalData.ndbMtTcWorkers, globalData.ndbMtTcThreads,
                      globalData.ndbMtSendThreads, globalData.ndbMtReceiveThreads,
                      globalData.ndbMtMainThreads);

  return 0;
}


static void
ndbd_exit(int code)
{
  // Don't allow negative return code
  if (code < 0)
    code = 255;

  NdbSleep_MilliSleep(1);

// gcov will not produce results when using _exit
#ifdef HAVE_GCOV
  exit(code);
#else
  _exit(code);
#endif
}


static FILE *angel_info_w = NULL;

static void
writeChildInfo(const char *token, int val)
{
  fprintf(angel_info_w, "%s=%d\n", token, val);
  fflush(angel_info_w);
}

void
childReportSignal(int signum)
{
  writeChildInfo("signal", signum);
}

static void
childExit(int error_code, int exit_code, Uint32 currentStartPhase)
{
  writeChildInfo("error", error_code);
  writeChildInfo("sphase", currentStartPhase);
  fprintf(angel_info_w, "\n");
  fclose(angel_info_w);
  ndbd_exit(exit_code);
}

static void
childAbort(int error_code, int exit_code, Uint32 currentStartPhase)
{
  writeChildInfo("error", error_code);
  writeChildInfo("sphase", currentStartPhase);
  fprintf(angel_info_w, "\n");
  fclose(angel_info_w);

#ifdef _WIN32
  // Don't use 'abort' on Windows since it returns
  // exit code 3 which conflict with NRT_NoStart_InitialStart
  ndbd_exit(exit_code);
#else
  signal(SIGABRT, SIG_DFL);
  abort();
#endif
}

extern "C"
void
handler_shutdown(int signum){
  g_eventLogger->info("Received signal %d. Performing stop.", signum);
  globalData.theGracefulShutdownFlag = true;
}

extern NdbMutex * theShutdownMutex;

extern "C"
void
handler_error(int signum){
  // only let one thread run shutdown
  static bool handling_error = false;
  static my_thread_t thread_id; // Valid when handling_error is true

  if (handling_error &&
      my_thread_equal(thread_id, my_thread_self()))
  {
    // Shutdown thread received signal
#ifndef _WIN32
	signal(signum, SIG_DFL);
    kill(getpid(), signum);
#endif
    while(true)
      NdbSleep_MilliSleep(10);
  }
  if(theShutdownMutex && NdbMutex_Trylock(theShutdownMutex) != 0)
    while(true)
      NdbSleep_MilliSleep(10);

  thread_id = my_thread_self();
  handling_error = true;

  g_eventLogger->info("Received signal %d. Running error handler.", signum);
  ndb_print_stacktrace();
  childReportSignal(signum);
  // restart the system
  char errorData[64], *info= 0;
#ifdef HAVE_STRSIGNAL
  info= strsignal(signum);
#endif
  BaseString::snprintf(errorData, sizeof(errorData), "Signal %d received; %s", signum,
		       info ? info : "No text for signal available");
  ERROR_SET_SIGNAL(fatal, NDBD_EXIT_OS_SIGNAL_RECEIVED, errorData, __FILE__);
}


static void
catchsigs(bool foreground){
  static const int signals_shutdown[] = {
#ifdef SIGBREAK
    SIGBREAK,
#endif
#ifdef SIGHUP
    SIGHUP,
#endif
    SIGINT,
#if defined SIGPWR
    SIGPWR,
#elif defined SIGINFO
    SIGINFO,
#endif
#ifdef _WIN32
    SIGTERM,
#else
    SIGQUIT,
#endif
    SIGTERM,
#ifdef SIGTSTP
    SIGTSTP,
#endif
#ifdef SIGTTIN
    SIGTTIN,
#endif
#ifdef SIGTTOU
    SIGTTOU
#endif
  };

  static const int signals_error[] = {
    SIGABRT,
#ifdef SIGALRM
    SIGALRM,
#endif
#ifdef SIGBUS
    SIGBUS,
#endif
    SIGFPE,
    SIGILL,
#ifdef SIGIO
    SIGIO,
#endif
#ifdef SIGPOLL
    SIGPOLL,
#endif
    SIGSEGV
  };

  static const int signals_ignore[] = {
#ifdef SIGCHLD
    /**
     * Ignore SIGCHLD  - we have no children,
     * but may inherit others' children
     */
    SIGCHLD,
#endif
#ifdef _WIN32
    SIGINT
#else
    SIGPIPE
#endif
  };

  size_t i;
  for(i = 0; i < sizeof(signals_shutdown)/sizeof(signals_shutdown[0]); i++)
    signal(signals_shutdown[i], handler_shutdown);
  for(i = 0; i < sizeof(signals_error)/sizeof(signals_error[0]); i++)
    signal(signals_error[i], handler_error);
  for(i = 0; i < sizeof(signals_ignore)/sizeof(signals_ignore[0]); i++)
    signal(signals_ignore[i], SIG_IGN);

#ifdef SIGTRAP
  if (!foreground)
    signal(SIGTRAP, handler_error);
#endif

}

#ifdef _WIN32
static HANDLE g_shutdown_event;

DWORD WINAPI shutdown_thread(LPVOID)
{
  // Wait forever until the shutdown event is signaled
  WaitForSingleObject(g_shutdown_event, INFINITE);

  g_eventLogger->info("Performing stop");
  globalData.theRestartFlag = perform_stop;
  return 0;
}
#endif

struct ThreadData
{
  FILE* f;
  LogBuffer* logBuf;
  bool stop;
};

/**
 * This function/thread is responsible for getting
 * bytes from the log buffer and writing them
 * to the log file.
 */

void* async_log_func(void* args)
{
  ThreadData* data = (ThreadData*)args;
  FILE* f = data->f;
  LogBuffer* logBuf = data->logBuf;
  const size_t get_bytes = 512;
  char buf[get_bytes + 1];
  size_t bytes;
  int part_bytes = 0, bytes_printed = 0;

  while(!data->stop)
  {
    part_bytes = 0;
    bytes_printed = 0;
    if((bytes = logBuf->get(buf, get_bytes)))
    {
      fwrite(buf, bytes, 1, f);
      fflush(f);
    }
  }

  while((bytes = logBuf->get(buf, get_bytes, 1)))// flush remaining logs
  {
    fwrite(buf, bytes, 1, f);
    fflush(f);
  }

  // print lost count in the end, if any
  size_t lost_count = logBuf->getLostCount();
  if(lost_count)
  {
    fprintf(f, LostMsgHandler::LOST_BYTES_FMT, lost_count);
    fflush(f);
  }

  return NULL;
}

static void log_memusage(const char* where=NULL)
{
#ifdef DEBUG_RSS
  const char* location = (where != NULL)?where : "Unknown";
  ndb_rusage ru;
  if (Ndb_GetRUsage(&ru, true) != 0)
  {
    g_eventLogger->error("Failed to get rusage");
  }
  else
  {
    g_eventLogger->info("ndbd.cpp %s : RSS : %llu kB", location, ru.ru_rss);
  }
#else
  (void)where;
#endif
}

void stop_async_log_func(NdbThread *thr, ThreadData& thr_args)
{
  if (thr)
  {
    void *dummy_return_status;
    thr_args.stop = true;
    NdbThread_WaitFor(thr, &dummy_return_status);
  }
}

void
ndbd_run(bool foreground, int report_fd,
         const char* connect_str, int force_nodeid, const char* bind_address,
         bool no_start, bool initial, bool initialstart,
         unsigned allocated_nodeid, int connect_retries, int connect_delay,
         size_t logbuffer_size)
{
  log_memusage("ndbd_run");
  LogBuffer* logBuf = new LogBuffer(logbuffer_size);
  BufferedOutputStream* ndbouts_bufferedoutputstream = new BufferedOutputStream(logBuf);

  // Make ndbout point to the BufferedOutputStream.
  NdbOut_ReInit(ndbouts_bufferedoutputstream, ndbouts_bufferedoutputstream);

  struct NdbThread* log_threadvar= NULL;
  ThreadData thread_args=
  {
    stdout,
    logBuf,
    false
  };
  // Create log thread.
  log_threadvar = NdbThread_Create(async_log_func,
                       (void**)&thread_args,
                       0,
                       (char*)"async_log_thread",
                       NDB_THREAD_PRIO_MEAN);
#ifdef _WIN32
  {
    char shutdown_event_name[32];
    _snprintf(shutdown_event_name, sizeof(shutdown_event_name),
              "ndbd_shutdown_%d", GetCurrentProcessId());

    g_shutdown_event = CreateEvent(NULL, true, false, shutdown_event_name);
    if (g_shutdown_event == NULL)
    {
      g_eventLogger->error("Failed to create shutdown event, error: %d",
                           GetLastError());
      stop_async_log_func(log_threadvar, thread_args);
      ndbd_exit(1);
    }

    HANDLE thread = CreateThread(NULL, 0, &shutdown_thread, NULL, 0, NULL);
    if (thread == NULL)
    {
      g_eventLogger->error("couldn't start shutdown thread, error: %d",
                           GetLastError());
      stop_async_log_func(log_threadvar, thread_args);
      ndbd_exit(1);
    }
  }
#endif

  ndb_init_stacktrace();

  if (foreground)
    g_eventLogger->info("Ndb started in foreground");

  if (report_fd)
  {
    g_eventLogger->debug("Opening report stream on fd: %d", report_fd);
    // Open a stream for sending extra status to angel
    if (!(angel_info_w = fdopen(report_fd, "w")))
    {
      g_eventLogger->error("Failed to open stream for reporting "
                           "to angel, error: %d (%s)", errno, strerror(errno));

      stop_async_log_func(log_threadvar, thread_args);
      ndbd_exit(-1);
    }
  }
  else
  {
    // No reporting requested, open /dev/null
    const char* dev_null = IF_WIN("nul", "/dev/null");
    if (!(angel_info_w = fopen(dev_null, "w")))
    {
      g_eventLogger->error("Failed to open stream for reporting to "
                           "'%s', error: %d (%s)", dev_null, errno,
                           strerror(errno));
      stop_async_log_func(log_threadvar, thread_args);
      ndbd_exit(-1);
    }
  }

  if (initialstart)
  {
    g_eventLogger->info("Performing partial initial start of this Cluster");
  }
  else if (initial)
  {
    g_eventLogger->info(
      "Initial start of data node, ignoring any info on disk");
  }
  else
  {
    g_eventLogger->info(
      "Normal start of data node using checkpoint and log info if existing");
  }

  log_memusage("init1");

  globalEmulatorData.create();

  globalEmulatorData.theThreadConfig->init_crash_mutex();

  log_memusage("Emulator init");

  Configuration* theConfig = globalEmulatorData.theConfiguration;
  if(!theConfig->init(no_start, initial, initialstart))
  {
    g_eventLogger->error("Failed to init Configuration");
    stop_async_log_func(log_threadvar, thread_args);
    ndbd_exit(-1);
  }

  log_memusage("Config init");

  /**
    Read the configuration from the assigned management server (could be
    a set of management servers, normally when we arrive here we have
    already assigned the nodeid, either by the operator or by the angel
    process.
  */
  theConfig->fetch_configuration(connect_str, force_nodeid, bind_address,
                                 allocated_nodeid, connect_retries,
                                 connect_delay);

  /**
    Set the NDB DataDir, this is where we will locate log files and data
    files unless specifically configured to be elsewhere.
  */
  g_eventLogger->info("Changing directory to '%s'",
                      NdbConfig_get_path(NULL));

  if (NdbDir::chdir(NdbConfig_get_path(NULL)) != 0)
  {
    g_eventLogger->warning("Cannot change directory to '%s', error: %d",
                           NdbConfig_get_path(NULL), errno);
    // Ignore error
  }

  log_memusage("Config fetch");

  theConfig->setupConfiguration();
  {
    /**
     * The minimum setting of TransactionMemory is the reserved transaction
     * memory and an additional 4 MByte (128 pages) per LDM and TC instance.
     */
    const ndb_mgm_configuration_iterator * p =
      globalEmulatorData.theConfiguration->getOwnConfigIterator();
    if (p == 0)
    {
      abort();
    }
    Uint64 reserved_transmem_bytes =
        globalEmulatorData.theSimBlockList->getTransactionMemoryNeed(
          globalData.ndbMtTcWorkers,
          globalData.ndbMtLqhWorkers,
          p);
    Uint64 min_transmem_bytes = reserved_transmem_bytes +
      (globalData.ndbMtTcWorkers + globalData.ndbMtLqhWorkers)
       * 128 * Uint64(32768);
    theConfig->setupMemoryConfiguration(min_transmem_bytes);
  }

  /**
    Printout various information about the threads in the
    run-time environment
  */
  if (get_multithreaded_config(globalEmulatorData))
  {
    stop_async_log_func(log_threadvar, thread_args);
    ndbd_exit(-1);
  }
  systemInfo(* theConfig, * theConfig->m_logLevel);

  /**
    Start the watch-dog thread before we start allocating memory.
    Allocation of memory can be a very time-consuming process.
    The watch-dog will have a special timeout for the phase where
    we allocate memory.
  */
  NdbThread* pWatchdog = globalEmulatorData.theWatchDog->doStart();

  log_memusage("Watchdog started");

  g_eventLogger->info("Memory Allocation for global memory pools Starting");
  {
    /*
     * Memory allocation can take a long time for large memory.
     *
     * So we want the watchdog to monitor the process of initial allocation.
     */
    Uint32 watchCounter;
    watchCounter = 9;           //  Means "doing allocation"
    globalEmulatorData.theWatchDog->registerWatchedThread(&watchCounter, 0);
    if (init_global_memory_manager(globalEmulatorData, &watchCounter) != 0)
    {
      stop_async_log_func(log_threadvar, thread_args);
      ndbd_exit(1);
    }
    globalEmulatorData.theWatchDog->unregisterWatchedThread(0);
  }
  g_eventLogger->info("Memory Allocation for global memory pools Completed");
  log_memusage("Global memory pools allocated");

  const ndb_mgm_configuration_iterator *p =
      globalEmulatorData.theConfiguration->getOwnConfigIterator();
  require(p != nullptr);

  bool have_password_option = g_filesystem_password_state.have_password_option();
  Uint32 encrypted_file_system = 0;
  ndb_mgm_get_int_parameter(p, CFG_DB_ENCRYPTED_FILE_SYSTEM,
                            &encrypted_file_system);
  if (have_password_option && encrypted_file_system == 0)
  {
    g_eventLogger->warning("Data node is not configured with "
        "EncryptedFileSystem=1, filesystem password will be ignored");
  }
  if (!have_password_option && encrypted_file_system == 1)
  {
    g_eventLogger->info("Data node configured to have encryption "
                         "but password not provided");
    ndbd_exit(-1);
  }

  if (have_password_option && encrypted_file_system == 1)
  {
    const char *pwd = g_filesystem_password_state.get_password();
    size_t pwd_size = g_filesystem_password_state.get_password_length();
    if(pwd_size <=0)
    {
      g_eventLogger->info("Invalid filesystem password, "
          "empty password not allowed");
      ndbd_exit(-1);
    }

    memcpy(globalData.filesystemPassword, pwd, pwd_size);
    globalData.filesystemPassword[pwd_size] = '\0';
    globalData.filesystemPasswordLength = pwd_size;
    require(globalData.filesystemPasswordLength> 0);
  }

  /**
    Initialise the data of the run-time environment, this prepares the
    data setup for the various threads that need to communicate using
    our internal memory. The threads haven't started yet, but as soon as
    they start they will be ready to communicate.
  */
  globalEmulatorData.theThreadConfig->init();

  globalEmulatorData.theConfiguration->addThread(log_threadvar, NdbfsThread);

  log_memusage("Thread config initialised");

#ifdef VM_TRACE
  // Initialize signal logger before block constructors
  char *signal_log_name = NdbConfig_SignalLogFileName(globalData.ownId);
  FILE * signalLog = fopen(signal_log_name, "a");
  if (signalLog)
  {
    globalSignalLoggers.setOutputStream(signalLog);
    globalSignalLoggers.setOwnNodeId(globalData.ownId);

    const char* p = NdbEnv_GetEnv("NDB_SIGNAL_LOG", (char*)0, 0);
    if (p != 0)
    {
      fprintf(signalLog, "START\n");
      fflush(signalLog);

      char buf[200];
      BaseString::snprintf(buf, sizeof(buf), "BLOCK=%s", p);
      for (char* q = buf; *q != 0; q++)
        *q = toupper(toascii(*q));
      g_eventLogger->info("Turning on signal logging using block spec.: '%s'", buf);
      globalSignalLoggers.log(SignalLoggerManager::LogInOut, buf);
      globalData.testOn = 1;
    }
  }
  else
  {
    // Failed to open signal log, print an error and ignore
    g_eventLogger->info("Failed to open signal logging file '%s', errno: %d",
                        signal_log_name, errno);
  }
  free(signal_log_name);
#endif

  /** Create all the blocks used by the run-time environment. */
  g_eventLogger->info("Loading blocks for data node run-time environment");
  // Load blocks (both main and workers)
  globalEmulatorData.theSimBlockList->load(globalEmulatorData);

  log_memusage("Load blocks completed");

  catchsigs(foreground);

  /**
   * Send the start signal to the CMVMI block. The start will however
   * not start until we have started the thread that runs the CMVMI
   * block. As soon as this thread starts it will find the signal
   * there to execute and we can start executing signals.
   */
  switch(globalData.theRestartFlag){
  case initial_state:
    globalEmulatorData.theThreadConfig->doStart(NodeState::SL_CMVMI);
    break;
  case perform_start:
    globalEmulatorData.theThreadConfig->doStart(NodeState::SL_CMVMI);
    globalEmulatorData.theThreadConfig->doStart(NodeState::SL_STARTING);
    break;
  default:
    assert("Illegal state globalData.theRestartFlag" == 0);
  }

  /**
    Before starting the run-time environment we also need to activate the
    send and receive services. We need for some cases to prepare some data
    in the TransporterRegistry before we start the communication service.
    The connection to the management server is reused as a connection to
    the management server node.
    The final steps is to start the client connections, these are all the
    nodes that we need to be client in the communication setup. Then start
    the socket server where other nodes can connect to us for those nodes
    where our node is the server part of the connection. The logic is that
    the node with the lower nodeid is the server and the other one the client,
    this can be changed by the configuration. This is implemented by the
    management server in the function fixPortNumber.
  */
  g_eventLogger->info("Starting Sending services");
  globalTransporterRegistry.startSending();
  g_eventLogger->info("Starting Receiving services");
  globalTransporterRegistry.startReceiving();
  g_eventLogger->info("Starting Transporter services");
  if (!globalTransporterRegistry.start_service(*globalEmulatorData.m_socket_server,
                                               globalData.theUseOnlyIPv4Flag))
  {
    g_eventLogger->info("globalTransporterRegistry.start_service() failed");
    stop_async_log_func(log_threadvar, thread_args);
    ndbd_exit(-1);
  }
  // Re-use the mgm handle as a transporter
  g_eventLogger->info("Reuse connection to NDB management server");
  if(!globalTransporterRegistry.connect_client(
		 theConfig->get_config_retriever()->get_mgmHandlePtr()))
      ERROR_SET(fatal, NDBD_EXIT_CONNECTION_SETUP_FAILED,
                "Failed to convert mgm connection to a transporter",
                __FILE__);
  g_eventLogger->info("Start transporter clients");
  NdbThread* pTrp = globalTransporterRegistry.start_clients();
  if (pTrp == 0)
  {
    stop_async_log_func(log_threadvar, thread_args);
    ndbd_exit(-1);
  }
  NdbThread* pSockServ = globalEmulatorData.m_socket_server->startServer();

  /**
    Report the new threads started, there is one thread started now to handle
    the watchdog, one to handle the socket server part and one to regularly
    attempt to connect as client to other nodes.
  */
  globalEmulatorData.theConfiguration->addThread(pTrp, SocketClientThread);
  globalEmulatorData.theConfiguration->addThread(pWatchdog, WatchDogThread);
  globalEmulatorData.theConfiguration->addThread(pSockServ, SocketServerThread);

  g_eventLogger->info("Starting the data node run-time environment");
  {
    /**
      We have finally arrived at the point where we start the run-time
      environment, in this method we will create the needed threads.
      We still have two different ThreadConfig objects, one to run
      ndbd (the single threaded variant of the data node process) and
      one to run ndbmtd (the multithreaded variant of the data node
      process).

      Mostly the ndbmtd should be used, but there could still be
      cases where someone prefers the single-threaded variant since
      this can provide lower latency if throughput isn't an issue.
    */
    NdbThread *pThis = NdbThread_CreateObject(0);
    globalEmulatorData.theThreadConfig->ipControlLoop(pThis);
  }
  g_eventLogger->info("The data node run-time environment has been stopped");

  /**
    The data node process is stopping, we remove the watchdog thread, the
    socket server and socket client thread from the list of running
    threads.
  */
  globalEmulatorData.theConfiguration->removeThread(pWatchdog);
  globalEmulatorData.theConfiguration->removeThread(pTrp);
  globalEmulatorData.theConfiguration->removeThread(pSockServ);

  NdbShutdown(0, NST_Normal);

  /**
   * Stopping the log thread is done at the very end since the
   * data node logs should be available until complete shutdown.
   */
  logBuf->stop();
  stop_async_log_func(log_threadvar, thread_args);
  globalEmulatorData.theConfiguration->removeThread(log_threadvar);
  NdbThread_Destroy(&log_threadvar);
  delete logBuf;
  delete ndbouts_bufferedoutputstream;
  ndbd_exit(0);
}

extern bool opt_core;

// instantiated and updated in NdbcntrMain.cpp
extern Uint32 g_currentStartPhase;

int simulate_error_during_shutdown= 0;

void
NdbShutdown(int error_code,
            NdbShutdownType type,
	    NdbRestartType restartType)
{
  globalData.theStopFlag = true;
  if(type == NST_ErrorInsert)
  {
    type = NST_Restart;
    restartType = (NdbRestartType)
      globalEmulatorData.theConfiguration->getRestartOnErrorInsert();
    if(restartType == NRT_Default)
    {
      type = NST_ErrorHandler;
      globalEmulatorData.theConfiguration->stopOnError(true);
    }
  }

  if((type == NST_ErrorHandlerSignal) || // Signal handler has already locked mutex
     (NdbMutex_Trylock(theShutdownMutex) == 0)){
    globalData.theRestartFlag = perform_stop;

    bool restart = false;

    if((type != NST_Normal &&
	globalEmulatorData.theConfiguration->stopOnError() == false) ||
       type == NST_Restart)
    {
      restart  = true;
    }

    const char * shutting = "shutting down";
    if(restart)
    {
      shutting = "restarting";
    }

    switch(type){
    case NST_Normal:
      g_eventLogger->info("Shutdown initiated");
      break;
    case NST_Watchdog:
      g_eventLogger->info("Watchdog %s system", shutting);
      break;
    case NST_ErrorHandler:
      g_eventLogger->info("Error handler %s system", shutting);
      break;
    case NST_ErrorHandlerSignal:
      g_eventLogger->info("Error handler signal %s system", shutting);
      break;
    case NST_Restart:
      g_eventLogger->info("Restarting system");
      break;
    default:
      g_eventLogger->info("Error handler %s system (unknown type: %u)",
                          shutting, (unsigned)type);
      type = NST_ErrorHandler;
      break;
    }

    const char * exitAbort = 0;
    if (opt_core)
      exitAbort = "aborting";
    else
      exitAbort = "exiting";

    if(type == NST_Watchdog)
    {
      /**
       * Very serious, don't attempt to free, just die!!
       */
      g_eventLogger->info("Watchdog shutdown completed - %s", exitAbort);
      if (opt_core)
      {
	childAbort(error_code, -1,g_currentStartPhase);
      }
      else
      {
	childExit(error_code, -1,g_currentStartPhase);
      }
    }

#ifndef _WIN32
    if (simulate_error_during_shutdown)
    {
      kill(getpid(), simulate_error_during_shutdown);
      while(true)
	NdbSleep_MilliSleep(10);
    }
#endif

    globalEmulatorData.theWatchDog->doStop();

#ifdef VM_TRACE
    FILE * outputStream = globalSignalLoggers.setOutputStream(0);
    if(outputStream != 0)
      fclose(outputStream);
#endif

    /**
     * Don't touch transporter here (yet)
     *   cause with ndbmtd, there are locks and nasty stuff
     *   and we don't know which we are holding...
     */
#ifdef NOT_YET

    /**
     * Stop all transporter connection attempts and accepts
     */
    globalEmulatorData.m_socket_server->stopServer();
    globalEmulatorData.m_socket_server->stopSessions();
    globalTransporterRegistry.stop_clients();

    /**
     * Stop transporter communication with other nodes
     */
    globalTransporterRegistry.stopSending();
    globalTransporterRegistry.stopReceiving();

    /**
     * Remove all transporters
     */
    globalTransporterRegistry.removeAll();
#endif

    if(type == NST_ErrorInsert && opt_core)
    {
      // Unload some structures to reduce size of core
      globalEmulatorData.theSimBlockList->unload();
      NdbMutex_Unlock(theShutdownMutex);
      globalEmulatorData.destroy();
    }

    if(type != NST_Normal && type != NST_Restart)
    {
      g_eventLogger->info("Error handler shutdown completed - %s", exitAbort);
      if (opt_core)
      {
	childAbort(error_code, -1,g_currentStartPhase);
      }
      else
      {
	childExit(error_code, -1,g_currentStartPhase);
      }
    }

    /**
     * This is a normal restart, depend on angel
     */
    if(type == NST_Restart){
      childExit(error_code, restartType,g_currentStartPhase);
    }

    g_eventLogger->info("Shutdown completed - exiting");
  }
  else
  {
    /**
     * Shutdown is already in progress
     */

    /**
     * If this is the watchdog, kill system the hard way
     */
    if (type== NST_Watchdog)
    {
      g_eventLogger->info("Watchdog is killing system the hard way");
#if defined VM_TRACE
      childAbort(error_code, -1,g_currentStartPhase);
#else
      childExit(error_code, -1, g_currentStartPhase);
#endif
    }

    while(true)
      NdbSleep_MilliSleep(10);
  }
}
