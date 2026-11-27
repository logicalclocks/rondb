/*
   Copyright (c) 2003, 2026, Oracle and/or its affiliates.
   Copyright (c) 2021, 2025, Hopsworks and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#define DBTUP_C
#define DBTUP_DEBUG_CPP
#include <ndb_limits.h>
#include <RefConvert.hpp>
#include <Vector.hpp>
#include <pc.hpp>
#include <signaldata/DropTab.hpp>
#include <signaldata/DumpStateOrd.hpp>
#include <signaldata/EventReport.hpp>
#include "Dbtup.hpp"
#include "DbtupJitGlue.hpp"   /* NdbJitStats / dbtup_jit_get_stats (Phase 8) */

#include <signaldata/DbinfoScan.hpp>
#include <signaldata/TransIdAI.hpp>
#include "AttributeOffset.hpp"
#include "../dblqh/Dblqh.hpp"
#ifdef DBTUP_HUGO_ROW_CHECK
#include <ndb_constants.h>
#include <signaldata/LqhKey.hpp>
#include <new>
#endif
#ifdef TEST_MR
#include <time.h>
#endif

#define JAM_FILE_ID 411

/* **************************************************************** */
/* ---------------------------------------------------------------- */
/* ------------------------ DEBUG MODULE -------------------------- */
/* ---------------------------------------------------------------- */
/* **************************************************************** */
void Dbtup::execDEBUG_SIG(Signal *signal) {
  PagePtr regPagePtr;
  jamEntry();
  regPagePtr.i = signal->theData[0];
  c_page_pool.getPtr(regPagePtr);
}  // Dbtup::execDEBUG_SIG()

#ifdef TEST_MR

void startTimer(struct timespec *tp) {
  clock_gettime(CLOCK_REALTIME, tp);
}  // startTimer()

int stopTimer(struct timespec *tp) {
  double timer_count;
  struct timespec theStopTime;
  clock_gettime(CLOCK_REALTIME, &theStopTime);
  timer_count =
      (double)(1000000 * ((double)theStopTime.tv_sec - (double)tp->tv_sec)) +
      (double)((double)((double)theStopTime.tv_nsec - (double)tp->tv_nsec) /
               (double)1000);
  return (int)timer_count;
}  // stopTimer()

#endif  // end TEST_MR

struct Chunk {
  Uint32 pageId;
  Uint32 pageCount;
};

void Dbtup::execDBINFO_SCANREQ(Signal *signal) {
  jamEntry();
  DbinfoScanReq req = *(DbinfoScanReq *)signal->theData;
  const Ndbinfo::ScanCursor *cursor =
      CAST_CONSTPTR(Ndbinfo::ScanCursor, DbinfoScan::getCursorPtr(&req));

  Ndbinfo::Ratelimit rl;

  switch (req.tableId) {
    case Ndbinfo::POOLS_TABLEID: {
      jam();
      const DynArr256Pool::Info pmpInfo = c_page_map_pool.getInfo();

      const Ndbinfo::pool_entry pools[] = {
          {"Scan Lock",
           c_scanLockPool.getUsed(),
           c_scanLockPool.getSize(),
           c_scanLockPool.getEntrySize(),
           c_scanLockPool.getUsedHi(),
           {CFG_DB_NO_LOCAL_SCANS, CFG_DB_BATCH_SIZE, 0, 0},
           0},
          {"Scan Operation",
           c_scanOpPool.getUsed(),
           c_scanOpPool.getSize(),
           c_scanOpPool.getEntrySize(),
           c_scanOpPool.getUsedHi(),
           {CFG_DB_NO_LOCAL_SCANS, 0, 0, 0},
           0},
          { "Trigger",
           cnoOfAllocatedTriggerRec,
           0,
           sizeof(TupTriggerData)/4,
           cnoOfMaxAllocatedTriggerRec,
           { CFG_DB_NO_TRIGGERS,0,0,0 },
             0},
          {"Stored Proc",
           c_storedProcPool.getUsed(),
           c_storedProcPool.getSize(),
           c_storedProcPool.getEntrySize(),
           c_storedProcPool.getUsedHi(),
           {CFG_DB_NO_LOCAL_SCANS, 0, 0, 0},
           0},
          {"Build Index",
           c_buildIndexPool.getUsed(),
           c_buildIndexPool.getSize(),
           c_buildIndexPool.getEntrySize(),
           c_buildIndexPool.getUsedHi(),
           {0, 0, 0, 0},
           0},
          {"Operation",
           c_operation_pool.getUsed(),
           c_operation_pool.getSize(),
           c_operation_pool.getEntrySize(),
           c_operation_pool.getUsedHi(),
           {CFG_DB_NO_LOCAL_OPS, CFG_DB_NO_OPS, 0, 0},
           0},
          {"L2PMap pages",
           pmpInfo.pg_count,
           0, /* No real limit */
           pmpInfo.pg_byte_sz,
           /*
             No HWM for this row as it would be a fixed fraction of "Data
             memory" and therefore of limited interest.
           */
           0,
           {0, 0, 0},
           RG_DATAMEM},
          {"L2PMap nodes",
           pmpInfo.inuse_nodes,
           pmpInfo.pg_count *
               pmpInfo.nodes_per_page, /* Max within current pages */
           pmpInfo.node_byte_sz,
           /*
             No HWM for this row as it would be a fixed fraction of "Data
             memory" and therefore of limited interest.
           */
           0,
           {0, 0, 0},
           RT_DBTUP_PAGE_MAP},
          {"Data memory",
           m_pages_allocated,
           0,  // Allocated from global resource group RG_DATAMEM
           sizeof(Page),
           m_pages_allocated_max,
           {CFG_DB_DATA_MEM, 0, 0, 0},
           0},
          {NULL, 0, 0, 0, 0, {0, 0, 0, 0}, 0}};

      const size_t num_config_params =
          sizeof(pools[0].config_params) / sizeof(pools[0].config_params[0]);
      const Uint32 numPools = NDB_ARRAY_SIZE(pools);
      Uint32 pool = cursor->data[0];
      ndbrequire(pool < numPools);
      BlockNumber bn = blockToMain(number());
      while (pools[pool].poolname) {
        jam();
        Ndbinfo::Row row(signal, req);
        row.write_uint32(getOwnNodeId());
        row.write_uint32(bn);          // block number
        row.write_uint32(instance());  // block instance
        row.write_string(pools[pool].poolname);

        row.write_uint64(pools[pool].used);
        row.write_uint64(pools[pool].total);
        row.write_uint64(pools[pool].used_hi);
        row.write_uint64(pools[pool].entry_size);
        for (size_t i = 0; i < num_config_params; i++)
          row.write_uint32(pools[pool].config_params[i]);
        row.write_uint32(GET_RG(pools[pool].record_type));
        row.write_uint32(GET_TID(pools[pool].record_type));
        ndbinfo_send_row(signal, req, row, rl);
        pool++;
        if (rl.need_break(req)) {
          jam();
          ndbinfo_send_scan_break(signal, req, rl, pool);
          return;
        }
      }
      break;
    }
    case Ndbinfo::TEST_TABLEID: {
      Uint32 counter = cursor->data[0];
      BlockNumber bn = blockToMain(number());
      while (counter < 1000) {
        jam();
        Ndbinfo::Row row(signal, req);
        row.write_uint32(getOwnNodeId());
        row.write_uint32(bn);          // block number
        row.write_uint32(instance());  // block instance
        row.write_uint32(counter);
        Uint64 counter2 = counter;
        counter2 = counter2 << 32;
        row.write_uint64(counter2);
        ndbinfo_send_row(signal, req, row, rl);
        counter++;
        if (rl.need_break(req)) {
          jam();
          ndbinfo_send_scan_break(signal, req, rl, counter);
          return;
        }
      }
      break;
    }
    case Ndbinfo::JIT_TABLEID: {
      jam();
      /* The JIT code-memory manager and program reuse caches are
       * node-global (shared across all LDM DBTUP instances), so emit
       * exactly one row per node from the first LDM worker rather than
       * one identical row per instance. */
      if (instance() == 1) {
        jam();
        NdbJitStats s;
        dbtup_jit_get_stats(&s);
        Ndbinfo::Row row(signal, req);
        row.write_uint32(getOwnNodeId());            // node_id
        row.write_uint64(s.code_reserved_bytes);     // code_reserved_bytes
        row.write_uint64(s.code_used_bytes);         // code_used_bytes
        row.write_uint32(s.code_slots_live);         // code_slots_live
        row.write_uint64(s.programs_compiled);       // programs_compiled
        row.write_uint64(s.programs_reused);         // programs_reused
        row.write_uint32(s.programs_cached);         // programs_cached
        row.write_uint64(s.programs_fallback);       // programs_fallback
        row.write_uint64(s.rows_executed);           // rows_executed
        row.write_uint64(s.compile_ns_total);        // compile_ns_total
        ndbinfo_send_row(signal, req, row, rl);
      }
      break;
    }
    default:
      break;
  }

  ndbinfo_send_scan_conf(signal, req, rl);
}

#ifdef VM_TRACE
static Uint32 fc_left = 0, fc_right = 0, fc_remove = 0;
#endif

void
Dbtup::execDUMP_STATE_ORD(Signal* signal)
{
  DumpStateOrd * const dumpState = (DumpStateOrd *)&signal->theData[0];
  Uint32 type = signal->theData[0];

  (void)type;
#if 0
  if (type == 100) {
    RelTabMemReq * const req = (RelTabMemReq *)signal->getDataPtrSend();
    req->primaryTableId = 2;
    req->secondaryTableId = RNIL;
    req->userPtr = 2;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_REL_TABMEMREQ, signal,
               RelTabMemReq::SignalLength, JBB);
    return;
  }//if
  if (type == 101) {
    RelTabMemReq * const req = (RelTabMemReq *)signal->getDataPtrSend();
    req->primaryTableId = 4;
    req->secondaryTableId = 5;
    req->userPtr = 4;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_REL_TABMEMREQ, signal,
               RelTabMemReq::SignalLength, JBB);
    return;
  }//if
  if (type == 102) {
    RelTabMemReq * const req = (RelTabMemReq *)signal->getDataPtrSend();
    req->primaryTableId = 6;
    req->secondaryTableId = 8;
    req->userPtr = 6;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_REL_TABMEMREQ, signal,
               RelTabMemReq::SignalLength, JBB);
    return;
  }//if
  if (type == 103) {
    DropTabFileReq * const req = (DropTabFileReq *)signal->getDataPtrSend();
    req->primaryTableId = 2;
    req->secondaryTableId = RNIL;
    req->userPtr = 2;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_DROP_TABFILEREQ, signal,
               DropTabFileReq::SignalLength, JBB);
    return;
  }//if
  if (type == 104) {
    DropTabFileReq * const req = (DropTabFileReq *)signal->getDataPtrSend();
    req->primaryTableId = 4;
    req->secondaryTableId = 5;
    req->userPtr = 4;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_DROP_TABFILEREQ, signal,
               DropTabFileReq::SignalLength, JBB);
    return;
  }//if
  if (type == 105) {
    DropTabFileReq * const req = (DropTabFileReq *)signal->getDataPtrSend();
    req->primaryTableId = 6;
    req->secondaryTableId = 8;
    req->userPtr = 6;
    req->userRef = DBDICT_REF;
    sendSignal(cownref, GSN_DROP_TABFILEREQ, signal,
               DropTabFileReq::SignalLength, JBB);
    return;
  }//if
#endif
#ifdef DBTUP_HUGO_ROW_CHECK
  if (dumpState->args[0] == DumpStateOrd::TupHugoRowCheck)
  {
    jam();
    /**
     * Needs nothing but the block itself, so a test can send it to a
     * restarted node while the node is held in no-start. The rows that
     * RESTORE, the REDO log and the copy fragment phase write are then
     * checked as well. Every instance receives it, the table id is per
     * data node process.
     */
    const Uint32 tableId =
        (signal->getLength() >= 2) ? signal->theData[1] : RNIL;
    g_dbtup_hugo_row_check_table.store(tableId, std::memory_order_relaxed);
    if (instance() == 1 && !m_is_query_block)
    {
      if (tableId == RNIL)
        g_eventLogger->info("Hugo row check off");
      else
        g_eventLogger->info("Hugo row check on table %u", tableId);
    }
    return;
  }
#endif
  if (dumpState->args[0] == DumpStateOrd::TupDumpJitPrograms)
  {
    jam();
    /* The JIT live-program registry is node-global (shared across all
     * LDM DBTUP instances) — dump once from the first LDM worker
     * rather than identically from every instance. */
    if (instance() == 1)
    {
      jam();
      dbtup_jit_dump_programs();
    }
    return;
  }
  if (dumpState->args[0] == DumpStateOrd::TupDumpOneScanRec)
  {
    Uint32 recordNo = RNIL;
    if (signal->length() == 2)
    {
      jam();
      recordNo = dumpState->args[1];
    }
    else
    {
      jam();
      return;
    }
    ScanOpPtr scanPtr;
    scanPtr.i = recordNo;
    if (!c_scanOpPool.getValidPtr(scanPtr))
    {
      jam();
      return;
    }
    jam();
    g_eventLogger->info("(%u)Dbtup::ScanRecord[%u]: state: %u, bits: %x",
                        instance(),
                        scanPtr.i,
                        scanPtr.p->m_state,
                        scanPtr.p->m_bits);
    g_eventLogger->info("tab(%u,%u), last_seen: %u, transid(%u,%u)",
                        scanPtr.p->m_tableId,
                        scanPtr.p->m_fragId,
                        scanPtr.p->m_last_seen,
                        scanPtr.p->m_transId1,
                        scanPtr.p->m_transId2);
    g_eventLogger->info("endPage: %u, scanGCI: %u, lock op: %u",
                        scanPtr.p->m_endPage,
                        scanPtr.p->m_scanGCI,
                        scanPtr.p->m_accLockOp);
    g_eventLogger->info("scanPos.m_get: %u, scanPos.m_key(%u,%u,%u)",
                        scanPtr.p->m_scanPos.m_get,
                        scanPtr.p->m_scanPos.m_key.m_page_no,
                        scanPtr.p->m_scanPos.m_key.m_page_idx,
                        scanPtr.p->m_scanPos.m_key.m_file_no);
  }
#ifdef ERROR_INSERT
  if (type == DumpStateOrd::EnableUndoDelayDataWrite) {
    DumpStateOrd *const dumpState = (DumpStateOrd *)&signal->theData[0];
    ndbout << "Dbtup:: delay write of datapages for table = "
           << dumpState->args[1] << endl;
    c_errorInsert4000TableId = dumpState->args[1];
    SET_ERROR_INSERT_VALUE(4000);
    return;
  }  // if
#endif
#if defined VM_TRACE
  if (type == 1211 || type == 1212 || type == 1213) {
    Uint32 seed = (Uint32)time(0);
    if (signal->getLength() > 1) seed = signal->theData[1];
    g_eventLogger->info("Startar modul test av Page Manager (seed: 0x%x)",
                        seed);
    srand(seed);

    Vector<Chunk> chunks;
    const Uint32 LOOPS = 1000;
    Uint32 sum_req = 0;
    Uint32 sum_conf = 0;
    Uint32 sum_loop = 0;
    Uint32 max_loop = 0;
    for (Uint32 i = 0; i < LOOPS; i++) {
      // Case
      Uint32 c = (rand() % 3);
      Resource_limit rl;
      m_ctx.m_mm.get_resource_limit(RG_DATAMEM, rl);
      const Uint32 free = rl.m_max - rl.m_curr;

      Uint32 alloc = 0;
      if (free <= 1) {
        c = 0;
        alloc = 1;
      } else
        alloc = 1 + (rand() % (free - 1));

      if (chunks.size() == 0 && c == 0) {
        c = 1 + rand() % 2;
      }

      if (type == 1211)
        g_eventLogger->info("loop=%d case=%d free=%d alloc=%d", i, c, free,
                            alloc);

      if (type == 1213) {
        c = 1;
        alloc = 2 + (sum_conf >> 3) + (sum_conf >> 4);
      }
      switch (c) {
        case 0: {  // Release
          const int ch = rand() % chunks.size();
          Chunk chunk = chunks[ch];
          chunks.erase(ch);
          returnCommonArea(chunk.pageId, chunk.pageCount);
        } break;
        case 2: {  // Seize(n) - fail
          alloc += free;
          sum_req += free;
          goto doalloc;
        }
        case 1: {  // Seize(n) (success)
          sum_req += alloc;
        doalloc:
          Chunk chunk;
          Tablerec tab(c_triggerPool);
          tab.m_allow_use_spare = false;
	  allocConsPages(jamBuffer(),
                         &tab,
                         alloc,
                         chunk.pageCount,
                         chunk.pageId);
          ndbrequire(chunk.pageCount <= alloc);
          if (chunk.pageCount != 0) {
            chunks.push_back(chunk);
            if (chunk.pageCount != alloc) {
              if (type == 1211)
                g_eventLogger->info(
                    "  Tried to allocate %d - only allocated %d - free: %d",
                    alloc, chunk.pageCount, free);
            }
          } else {
            g_eventLogger->info("  Failed to alloc %d pages with %d pages free",
                                alloc, free);
          }

          sum_conf += chunk.pageCount;
          Uint32 tot = fc_left + fc_right + fc_remove;
          sum_loop += tot;
          if (tot > max_loop) max_loop = tot;

          for (Uint32 i = 0; i < chunk.pageCount; i++) {
            PagePtr pagePtr;
            pagePtr.i = chunk.pageId + i;
            c_page_pool.getPtr(pagePtr);
          }

          if (alloc == 1 && free > 0) {
            ndbrequire(chunk.pageCount == alloc);
          }
        } break;
      }
    }
    while (chunks.size() > 0) {
      Chunk chunk = chunks.back();
      returnCommonArea(chunk.pageId, chunk.pageCount);
      chunks.erase(chunks.size() - 1);
    }

    g_eventLogger->info(
        "Got %u%% of requested allocs, loops : %u 100*avg: %u max: %u",
        (100 * sum_conf) / sum_req, sum_loop, 100 * sum_loop / LOOPS, max_loop);
  }
#endif

#ifdef ERROR_INSERT
  if (signal->theData[0] == DumpStateOrd::SchemaResourceSnapshot) {
    {
      Uint64 defaultValueWords = 0;
      if (DefaultValuesFragment.i != RNIL)
        defaultValueWords = calculate_used_var_words(DefaultValuesFragment.p);
      Uint32 defaultValueWordsHi = (Uint32)(defaultValueWords >> 32);
      Uint32 defaultValueWordsLo = (Uint32)(defaultValueWords & 0xFFFFFFFF);
      RSS_OP_SNAPSHOT_SAVE(defaultValueWordsHi);
      RSS_OP_SNAPSHOT_SAVE(defaultValueWordsLo);
    }
    g_eventLogger->info("(%u)SAVE TUP: cnoOfAllocatedFragrec: %u", instance(), cnoOfAllocatedFragrec);
    RSS_OP_SNAPSHOT_SAVE(cnoOfFreeFragoprec);
    RSS_OP_SNAPSHOT_SAVE(cnoOfAllocatedFragrec);
    RSS_OP_SNAPSHOT_SAVE(cnoOfFreeTabDescrRec);

    RSS_AP_SNAPSHOT_SAVE2(c_storedProcPool, c_storedProcCountNonAPI);
    return;
  }

  if (signal->theData[0] == DumpStateOrd::SchemaResourceCheckLeak) {
    {
      Uint64 defaultValueWords = 0;
      if (DefaultValuesFragment.i != RNIL)
        defaultValueWords = calculate_used_var_words(DefaultValuesFragment.p);
      Uint32 defaultValueWordsHi = (Uint32)(defaultValueWords >> 32);
      Uint32 defaultValueWordsLo = (Uint32)(defaultValueWords & 0xFFFFFFFF);
      RSS_OP_SNAPSHOT_CHECK(defaultValueWordsHi);
      RSS_OP_SNAPSHOT_CHECK(defaultValueWordsLo);
    }
    g_eventLogger->info("(%u)CHECK TUP: cnoOfAllocatedFragrec: %u", instance(), cnoOfAllocatedFragrec);
    RSS_OP_SNAPSHOT_CHECK(cnoOfFreeFragoprec);
    RSS_OP_SNAPSHOT_CHECK(cnoOfAllocatedFragrec);
    RSS_OP_SNAPSHOT_CHECK(cnoOfFreeTabDescrRec);

    RSS_AP_SNAPSHOT_CHECK2(c_storedProcPool, c_storedProcCountNonAPI);
    return;
  }
#endif
#ifdef ERROR_INSERT
  if (type == DumpStateOrd::TupSaveCopyTupleCount) {
    jam();
    m_copy_tuple_saved_count = m_copy_tuple_alloc_count;
    return;
  }
  if (type == DumpStateOrd::TupCheckCopyTupleCount) {
    jam();
    ndbrequire(m_copy_tuple_alloc_count == m_copy_tuple_saved_count);
    return;
  }
#endif
#if defined(VM_TRACE) || defined(ERROR_INSERT)
  if (type == DumpStateOrd::TupSetTransientPoolMaxSize) {
    jam();
    if (signal->getLength() < 3) return;
    const Uint32 pool_index = signal->theData[1];
    const Uint32 new_size = signal->theData[2];
    if (pool_index == DBTUP_OPERATION_RECORD_TRANSIENT_POOL_INDEX)
    {
      c_lqh->set_error_value(5099);
    }
    if (pool_index >= c_transient_pool_count)
      return;
    c_transient_pools[pool_index]->setMaxSize(new_size);
    return;
  }
  if (type == DumpStateOrd::TupResetTransientPoolMaxSize) {
    jam();
    if (signal->getLength() < 2) return;
    const Uint32 pool_index = signal->theData[1];
    if (pool_index == DBTUP_OPERATION_RECORD_TRANSIENT_POOL_INDEX)
    {
      c_lqh->set_error_value(0);
    }
    if (pool_index >= c_transient_pool_count)
      return;
    c_transient_pools[pool_index]->resetMaxSize();
    return;
  }
#endif
}  // Dbtup::execDUMP_STATE_ORD()

#ifdef DBTUP_HUGO_ROW_CHECK
std::atomic<Uint32> g_dbtup_hugo_row_check_table{RNIL};

/**
 * Hugo row check
 * --------------
 * The NDBT test programs write rows through HugoCalculator: every column
 * is a function of the row's id (the first Unsigned column) and of its
 * update count (the last Unsigned column that is not part of the primary
 * key). A row that does not match that function was written wrong, by
 * the data node or by the test.
 *
 * With DUMP 2384 <tableId> every insert and update of that table is
 * checked as soon as DBTUP has prepared the new row version, whatever sent
 * the operation: the API, a scan take-over, the previous replica, the copy
 * fragment phase, RESTORE or the REDO log. A mismatch logs the operation
 * and crashes the node where the wrong row is written.
 *
 * A test case enables it only when all its own writes follow
 * HugoCalculator, and turns it off at the end.
 *
 * The functions below are HugoCalculator::calcValue() and its helpers
 * (storage/ndb/test/src/HugoCalculator.cpp) and must be kept equal to it.
 */
static Uint32 hugo_rand(Uint64 *seed) {
  const Uint64 mul = 0x5deece66dull;
  const Uint64 add = 0xb;
  const Uint64 loc_result = *seed * mul + add;
  *seed = loc_result;
  return (Uint32)(loc_result >> 1);
}

static Uint32 hugo_calc_len(Uint32 rvalue, Uint32 maxlen) {
  Uint32 minlen;
  if ((rvalue >> 16) < 4096)
    minlen = 15;
  else if ((rvalue >> 16) < 8192)
    minlen = 25;
  else if ((rvalue >> 16) < 16384)
    minlen = 35;
  else
    minlen = 64;
  if (maxlen <= minlen) return maxlen;
  /* Ensure coverage of maxlen */
  if ((rvalue & 64) == 0) return maxlen;
  return minlen + (rvalue % (maxlen - minlen));
}

static const char hugo_base64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789+/";

static Uint32 hugo_seed_sum(Uint32 record, Uint32 attrib, Uint32 updates,
                            bool pk) {
  return pk ? record + attrib : record + attrib + updates;
}

/**
 * The value Hugo writes in a column. Returns false for NULL. bytes is the
 * column size as NdbDictionary::Column::getSizeInBytes() gives it, bits
 * the length of a Bit column. Blob and Text columns are not handled.
 */
static bool hugo_calc_value(Uint32 record, Uint32 attrib, Uint32 updates,
                            Uint32 type, bool pk, bool nullable, Uint32 bytes,
                            Uint32 bits, Uint8 *buf, Uint32 *outlen) {
  /* Hugo adds ints and assigns the sum to a Uint64, which sign-extends */
  Uint64 seed =
      (Uint64)(Int64)(Int32)hugo_seed_sum(record, attrib, updates, pk);
  Uint32 val = hugo_rand(&seed);
  *outlen = bytes;
  if (nullable && ((val >> 16) & 255) > 220) {
    *outlen = 0;
    return false;
  }
  Uint32 len = bytes;
  Uint32 pos = 0;
  Uint8 *dst = buf;
  switch (type) {
    case NDB_TYPE_FLOAT: {
      const float x = (float)hugo_rand(&seed);
      memcpy(buf, &x, 4);
      return true;
    }
    case NDB_TYPE_DOUBLE: {
      const double x = (double)hugo_rand(&seed);
      memcpy(buf, &x, 8);
      return true;
    }
    case NDB_TYPE_VARCHAR:
    case NDB_TYPE_VARBINARY:
      len = hugo_calc_len(hugo_rand(&seed), bytes - 1);
      *outlen = len + 1;
      buf[0] = (Uint8)len;
      dst = buf + 1;
      break;
    case NDB_TYPE_LONGVARCHAR:
    case NDB_TYPE_LONGVARBINARY:
      len = hugo_calc_len(hugo_rand(&seed), bytes - 2);
      *outlen = len + 2;
      buf[0] = (Uint8)(len & 0xff);
      buf[1] = (Uint8)(len >> 8);
      dst = buf + 2;
      break;
    case NDB_TYPE_CHAR:
      break;
    default:
      /* Integer, decimal, binary, bit, date and time types */
      while (len > 4) {
        memcpy(buf + pos, &val, 4);
        pos += 4;
        len -= 4;
        val = hugo_rand(&seed);
      }
      memcpy(buf + pos, &val, len);
      if (type == NDB_TYPE_BIT && (bits & 31) != 0) {
        /* At a multiple of 32 bits Hugo masks a word after the column */
        Uint32 last;
        memcpy(&last, buf + 4 * (bits >> 5), 4);
        last &= ((1U << (bits & 31)) - 1);
        memcpy(buf + 4 * (bits >> 5), &last, 4);
      }
      return true;
  }
  /* Characters, also of the var types: base64 of the random values */
  const Uint8 *ptr = (const Uint8 *)&val;
  while (len >= 4) {
    len -= 4;
    dst[pos++] = hugo_base64[ptr[0] & 0x3f];
    dst[pos++] = hugo_base64[ptr[1] & 0x3f];
    dst[pos++] = hugo_base64[ptr[2] & 0x3f];
    dst[pos++] = hugo_base64[ptr[3] & 0x3f];
    val = hugo_rand(&seed);
  }
  /* Hugo takes the last characters from bytes len..1 of the value */
  for (; len; len--, pos++) dst[pos] = hugo_base64[ptr[len] & 0x3f];
  return true;
}

/**
 * The two seed sums (record + attrib + updates) whose first random value
 * is value, to name the record a wrong Unsigned column belongs to.
 */
static void hugo_seed_sums(Uint32 value, Uint32 sums[2]) {
  const Uint64 mul = 0x5deece66dull;
  Uint64 inv = mul; /* Newton iteration, correct to 96 bits after 5 steps */
  for (Uint32 i = 0; i < 5; i++) inv *= 2 - mul * inv;
  for (Uint32 b = 0; b < 2; b++) {
    const Uint64 x = ((Uint64)value << 1) | b;
    sums[b] = (Uint32)((x - 0xb) * inv);
  }
}

/* Hex of the first bytes, ".." when cut */
static void hugo_hex(char *out, size_t outsz, const Uint8 *data, Uint32 len) {
  size_t pos = 0;
  Uint32 i = 0;
  out[0] = 0;
  for (; i < len && pos + 5 < outsz; i++) {
    pos += snprintf(out + pos, outsz - pos, "%02x", data[i]);
  }
  if (i < len) snprintf(out + pos, outsz - pos, "..");
}

void Dbtup::hugo_row_check(KeyReqStruct *req_struct, Operationrec *regOperPtr,
                           Tablerec *regTabPtr, Fragrecord *regFragPtr,
                           bool disk, const void *lqhOpPtrP_void) {
  jam();
  const Dblqh::TcConnectionrec *lqhOpPtrP =
      static_cast<const Dblqh::TcConnectionrec *>(lqhOpPtrP_void);
  const Uint32 noOfAttrs = regTabPtr->m_no_of_attributes;
  const Uint32 *tab_descr = regTabPtr->tabDescriptor;

  /* The id and update count columns, found as HugoCalculator finds them */
  Uint32 idCol = RNIL;
  Uint32 updCol = RNIL;
  for (Uint32 i = 0; i < noOfAttrs; i++) {
    const Uint32 desc = tab_descr[i * ZAD_SIZE];
    if (AttributeDescriptor::getType(desc) != NDB_TYPE_UNSIGNED) continue;
    if (idCol == RNIL) idCol = i;
    if (!AttributeDescriptor::getPrimaryKey(desc)) updCol = i;
  }
  if (idCol == RNIL || updCol == RNIL || idCol == updCol) {
    static std::atomic<bool> warned{false};
    if (!warned.exchange(true)) {
      g_eventLogger->warning(
          "Hugo row check: table %u has no Hugo id and update count"
          " columns, its rows are not checked",
          regFragPtr->fragTableId);
    }
    return;
  }

  static constexpr Uint32 IN_WORDS = MAX_ATTRIBUTES_IN_TABLE;
  static constexpr Uint32 OFF_WORDS = MAX_ATTRIBUTES_IN_TABLE;
  static constexpr Uint32 OUT_WORDS =
      MAX_TUPLE_SIZE_IN_WORDS + MAX_ATTRIBUTES_IN_TABLE;
  static constexpr Uint32 EXP_WORDS = MAX_TUPLE_SIZE_IN_WORDS + 2;
  if (m_hugo_row_check_buf == nullptr) {
    m_hugo_row_check_buf = new (std::nothrow)
        Uint32[IN_WORDS + OFF_WORDS + OUT_WORDS + EXP_WORDS];
    if (m_hugo_row_check_buf == nullptr) return;
  }
  Uint32 *const inBuf = m_hugo_row_check_buf;
  Uint32 *const offs = inBuf + IN_WORDS;
  Uint32 *const outBuf = offs + OFF_WORDS;
  Uint8 *const expected = (Uint8 *)(outBuf + OUT_WORDS);

  /**
   * Disk columns can only be read when the disk part is available, as
   * for the immediate triggers. Blob and Text columns are not checked.
   */
  const bool readDisk = disk && regTabPtr->m_no_of_disk_attributes > 0;
  Uint32 numIn = 0;
  for (Uint32 i = 0; i < noOfAttrs; i++) {
    const Uint32 desc = tab_descr[i * ZAD_SIZE];
    const Uint32 type = AttributeDescriptor::getType(desc);
    offs[i] = RNIL;
    if (type == NDB_TYPE_BLOB || type == NDB_TYPE_TEXT ||
        type == NDB_TYPE_UNDEFINED) {
      continue;
    }
    if (AttributeDescriptor::getDiskBased(desc) && !readDisk) continue;
    AttributeHeader::init(&inBuf[numIn++], i, 0);
  }

  /**
   * Read the new row version as readTriggerInfo reads the after values,
   * and leave req_struct as it was for the rest of execTUPKEYREQ.
   */
  Tuple_header *const save_tuple_ptr = req_struct->m_tuple_ptr;
  Tuple_header *const save_disk_ptr = req_struct->m_disk_ptr;
  const PagePtr save_disk_page_ptr = req_struct->m_disk_page_ptr;
  KeyReqStruct::Var_data save_var_data[2];
  memcpy(save_var_data, req_struct->m_var_data, sizeof(save_var_data));
  const bool save_is_expanded = req_struct->is_expanded;
  Tablerec *const save_tablePtrP = req_struct->tablePtrP;
  Operationrec *const save_operPtrP = req_struct->operPtrP;
  Uint32 *const save_attr_descr = req_struct->attr_descr;
  const Uint32 save_check_offset_mm = req_struct->check_offset[MM];
  const Uint32 save_check_offset_dd = req_struct->check_offset[DD];
  const Uint32 save_max_read = req_struct->max_read;
  const Uint32 save_out_buf_index = req_struct->out_buf_index;
  const Uint32 save_out_buf_bits = req_struct->out_buf_bits;
  const Uint32 save_partial_size = req_struct->partial_size;
  const bool save_xfrm_flag = req_struct->xfrm_flag;

  req_struct->tablePtrP = regTabPtr;
  req_struct->operPtrP = regOperPtr;
  req_struct->check_offset[MM] = regTabPtr->get_check_offset(MM);
  req_struct->check_offset[DD] = regTabPtr->get_check_offset(DD);
  req_struct->attr_descr = regTabPtr->tabDescriptor;
  if (regTabPtr->need_expand(readDisk)) {
    if (req_struct->m_disk_page_ptr.i != RNIL &&
        regOperPtr->m_disk_extra_callback_page != RNIL) {
      /* As in readTriggerInfo: the disk part is on the original page */
      req_struct->m_disk_page_ptr.i = regOperPtr->m_disk_callback_page;
    }
    prepare_read(req_struct, regTabPtr, readDisk);
  }
  const int ret = readAttributes(req_struct, inBuf, numIn, outBuf, OUT_WORDS);

  req_struct->m_tuple_ptr = save_tuple_ptr;
  req_struct->m_disk_ptr = save_disk_ptr;
  req_struct->m_disk_page_ptr = save_disk_page_ptr;
  memcpy(req_struct->m_var_data, save_var_data, sizeof(save_var_data));
  req_struct->is_expanded = save_is_expanded;
  req_struct->tablePtrP = save_tablePtrP;
  req_struct->operPtrP = save_operPtrP;
  req_struct->attr_descr = save_attr_descr;
  req_struct->check_offset[MM] = save_check_offset_mm;
  req_struct->check_offset[DD] = save_check_offset_dd;
  req_struct->max_read = save_max_read;
  req_struct->out_buf_index = save_out_buf_index;
  req_struct->out_buf_bits = save_out_buf_bits;
  req_struct->partial_size = save_partial_size;
  req_struct->xfrm_flag = save_xfrm_flag;

  if (ret < 0) {
    jam();
    g_eventLogger->warning(
        "Hugo row check: reading tab(%u,%u) failed with error %d, row not"
        " checked",
        regFragPtr->fragTableId, regFragPtr->fragmentId, -ret);
    return;
  }
  for (Uint32 pos = 0; pos < (Uint32)ret;) {
    const AttributeHeader ah(outBuf[pos]);
    if (ah.getAttributeId() < noOfAttrs) offs[ah.getAttributeId()] = pos;
    pos += 1 + ah.getDataSize();
  }
  if (offs[idCol] == RNIL || offs[updCol] == RNIL) {
    /* Disk based id or update count column without the disk part */
    return;
  }
  const AttributeHeader idAh(outBuf[offs[idCol]]);
  const AttributeHeader updAh(outBuf[offs[updCol]]);
  if (idAh.isNULL() || updAh.isNULL()) {
    /* Hugo never writes NULL in these columns */
    g_eventLogger->error(
        "Hugo row check failed: tab(%u,%u) instance %u rowid(%u,%u) TUP op"
        " %u: id (attribute %u) or update count (attribute %u) is NULL",
        regFragPtr->fragTableId, regFragPtr->fragmentId, instance(),
        regOperPtr->fragPageId, regOperPtr->m_tuple_location.m_page_idx,
        (Uint32)regOperPtr->op_type, idCol, updCol);
    ndbabort();
  }
  const Uint32 record = outBuf[offs[idCol] + 1];
  const Uint32 updates = outBuf[offs[updCol] + 1];

  Uint32 wrongCols = 0;
  Uint32 firstWrong = RNIL;
  Uint32 firstExpLen = 0;
  bool firstExpNull = false;
  for (Uint32 i = 0; i < noOfAttrs; i++) {
    if (i == idCol || i == updCol || offs[i] == RNIL) continue;
    const Uint32 desc = tab_descr[i * ZAD_SIZE];
    const Uint32 type = AttributeDescriptor::getType(desc);
    const Uint32 bits = AttributeDescriptor::getArraySize(desc);
    const Uint32 bytes = (type == NDB_TYPE_BIT)
                             ? ((bits + 31) >> 5) * 4
                             : AttributeDescriptor::getSizeInBytes(desc);
    if (bytes > 4 * (EXP_WORDS - 2)) continue;
    Uint32 expLen = 0;
    const bool notNull = hugo_calc_value(
        record, i, updates, type, AttributeDescriptor::getPrimaryKey(desc),
        AttributeDescriptor::getNullable(desc), bytes, bits, expected,
        &expLen);
    const AttributeHeader ah(outBuf[offs[i]]);
    const Uint8 *stored = (const Uint8 *)&outBuf[offs[i] + 1];
    const bool ok = notNull
                        ? (!ah.isNULL() && ah.getByteSize() == expLen &&
                           memcmp(stored, expected, expLen) == 0)
                        : ah.isNULL();
    if (ok) continue;
    if (wrongCols++ == 0) {
      firstWrong = i;
      firstExpLen = expLen;
      firstExpNull = !notNull;
    }
  }
  if (likely(wrongCols == 0)) return;

  /* Report the first wrong column, recompute its expected value */
  const Uint32 desc = tab_descr[firstWrong * ZAD_SIZE];
  const Uint32 type = AttributeDescriptor::getType(desc);
  const bool pk = AttributeDescriptor::getPrimaryKey(desc);
  const Uint32 bits = AttributeDescriptor::getArraySize(desc);
  const Uint32 bytes = (type == NDB_TYPE_BIT)
                           ? ((bits + 31) >> 5) * 4
                           : AttributeDescriptor::getSizeInBytes(desc);
  Uint32 expLen = 0;
  hugo_calc_value(record, firstWrong, updates, type, pk,
                  AttributeDescriptor::getNullable(desc), bytes, bits,
                  expected, &expLen);
  const AttributeHeader ah(outBuf[offs[firstWrong]]);
  const Uint8 *stored = (const Uint8 *)&outBuf[offs[firstWrong] + 1];
  char expHex[80];
  char storedHex[80];
  hugo_hex(expHex, sizeof(expHex), expected, firstExpNull ? 0 : firstExpLen);
  hugo_hex(storedHex, sizeof(storedHex), stored,
           ah.isNULL() ? 0 : ah.getByteSize());

  g_eventLogger->error(
      "Hugo row check failed: tab(%u,%u) instance %u rowid(%u,%u) TUP op %u"
      " id %u update count %u: %u wrong columns, first is attribute %u"
      " type %u",
      regFragPtr->fragTableId, regFragPtr->fragmentId, instance(),
      regOperPtr->fragPageId, regOperPtr->m_tuple_location.m_page_idx,
      (Uint32)regOperPtr->op_type, record, updates, wrongCols, firstWrong,
      type);
  g_eventLogger->error("  expected%s len %u: %s", firstExpNull ? " NULL" : "",
                       firstExpNull ? 0 : firstExpLen, expHex);
  g_eventLogger->error("  stored%s len %u: %s", ah.isNULL() ? " NULL" : "",
                       ah.isNULL() ? 0 : ah.getByteSize(), storedHex);
  if (type == NDB_TYPE_UNSIGNED && !pk && !ah.isNULL()) {
    /* Which id has this value with the stored update count */
    Uint32 sums[2];
    hugo_seed_sums(*(const Uint32 *)stored, sums);
    g_eventLogger->error(
        "  stored value is the value of id %u or id %u with this update"
        " count",
        sums[0] - firstWrong - updates, sums[1] - firstWrong - updates);
  }
  if (lqhOpPtrP != nullptr) {
    g_eventLogger->error(
        "  LQH op %u transid(0x%x,0x%x) from block %u node %u seqNoReplica"
        " %u lastReplicaNo %u scan take-over %u NR copy %u REDO %u"
        " activeCreat %u dirty %u rowid flag %u wire rowid(%u,%u)",
        (Uint32)lqhOpPtrP->operation, lqhOpPtrP->transid[0],
        lqhOpPtrP->transid[1], refToMain(lqhOpPtrP->tcBlockref),
        refToNode(lqhOpPtrP->tcBlockref), (Uint32)lqhOpPtrP->seqNoReplica,
        (Uint32)lqhOpPtrP->lastReplicaNo, (Uint32)lqhOpPtrP->indTakeOver,
        LqhKeyReq::getNrCopyFlag(lqhOpPtrP->reqinfo),
        c_lqh->c_executing_redo_log, (Uint32)lqhOpPtrP->activeCreat,
        (Uint32)lqhOpPtrP->dirtyOp, (Uint32)req_struct->m_use_rowid,
        req_struct->m_row_id.m_page_no, req_struct->m_row_id.m_page_idx);
  }
  ndbabort();
}
#endif  // DBTUP_HUGO_ROW_CHECK

/* ---------------------------------------------------------------- */
/* ---------      MEMORY       CHECK        ----------------------- */
/* ---------------------------------------------------------------- */
void Dbtup::execMEMCHECKREQ(Signal *signal) {
  TablerecPtr regTabPtr;
  regTabPtr.i = 2;
  ptrCheckGuard(regTabPtr, cnoOfTablerec, tablerec);
  if(tablerec && regTabPtr.p->tableStatus == DEFINED)
    validate_page(regTabPtr, 0);

#if 0
  const Dbtup::Tablerec& tab = *tup->tabptr.p;

  PagePtr regPagePtr;
  Uint32* data = &signal->theData[0];

  jamEntry();
  BlockReference blockref = signal->theData[0];
  Uint32 i;
  for (i = 0; i < 25; i++) {
    jam();
    data[i] = 0;
  }//for
  for (i = 0; i < 16; i++) {
    regPagePtr.i = cfreepageList[i];
    jam();
    while (regPagePtr.i != RNIL) {
      jam();
      ptrCheckGuard(regPagePtr, cnoOfPage, cpage);
      regPagePtr.i = regPagePtr.p->next_page;
      data[0]++;
    }//while
  }//for
  sendSignal(blockref, GSN_MEMCHECKCONF, signal, 25, JBB);
#endif
}  // Dbtup::memCheck()

#ifdef VM_TRACE
NdbOut &operator<<(NdbOut &out, const Dbtup::Operationrec &op) {
  out << "[Operationrec " << hex << &op;
  // table
  out << " [fragmentPtr " << hex << op.fragmentPtr << "]";
  // type
  out << " [op_type " << dec << op.op_type << "]";
  out << " [delete_insert_flag " << dec;
  out << op.op_struct.bit_field.delete_insert_flag << "]";
  // state
  out << " [tuple_state " << dec << op.tuple_state << "]";
  out << " [trans_state " << dec << op.trans_state << "]";
  out << " [in_active_list " << dec << op.op_struct.bit_field.in_active_list
      << "]";
  // links
  out << " [prevActiveOp " << hex << op.prevActiveOp << "]";
  out << " [nextActiveOp " << hex << op.nextActiveOp << "]";
  // tuples
  out << " [tupVersion " << hex << op.op_struct.bit_field.tupVersion << "]";
  out << " [m_tuple_location " << op.m_tuple_location << "]";
  out << " [m_copy_tuple_location " << hex << op.m_copy_tuple_location << "]";
  out << "]";
  return out;
}

// uses global tabptr
NdbOut &operator<<(NdbOut &out, const Dbtup::Th &th) {
  unsigned i = 0;
  out << "[Th " << hex << &th;
  out << " [op " << hex << th.data[i++] << "]";
  out << " [version " << hex << (Uint16)th.data[i++] << "]";
  out << "]";
  return out;
}
#endif

#ifdef VM_TRACE
template class Vector<Chunk>;
#endif
// uses global tabptr

NdbOut &operator<<(NdbOut &out, const Local_key &key) {
  out << "[ m_page_no: " << dec << key.m_page_no << " m_file_no: " << dec
      << key.m_file_no << " m_page_idx: " << dec << key.m_page_idx << "]";
  return out;
}

char *printLocal_Key(char buf[], int bufsize, const Local_key &key) {
  BaseString::snprintf(buf, bufsize,
                       "[ m_page_no: %u m_file_no: %u m_page_idx: %u ]",
                       key.m_page_no, key.m_file_no, key.m_page_idx);
  return buf;
}

static NdbOut &operator<<(NdbOut &out,
                          const Dbtup::Tablerec::Tuple_offsets &off) {
  out << "[ null_words: " << (Uint32)off.m_null_words
      << " null off: " << (Uint32)off.m_null_offset
      << " disk_off: " << off.m_disk_ref_offset
      << " fixheadsz: " << off.m_fix_header_size
      << " max_var_off: " << off.m_max_var_offset << " ]";

  return out;
}

NdbOut &operator<<(NdbOut &out, const Dbtup::Tablerec &tab) {
  out << "[ total_rec_size: " << tab.total_rec_size
      << " checksum: " << !!(tab.m_bits & Dbtup::Tablerec::TR_Checksum)
      << " attr: " << tab.m_no_of_attributes
      << " disk: " << tab.m_no_of_disk_attributes
      << " mm: " << tab.m_offsets[Dbtup::MM]
      << " [ fix: " << tab.m_attributes[Dbtup::MM].m_no_of_fixsize
      << " var: " << tab.m_attributes[Dbtup::MM].m_no_of_varsize << "]"

      << " dd: " << tab.m_offsets[Dbtup::DD]
      << " [ fix: " << tab.m_attributes[Dbtup::DD].m_no_of_fixsize
      << " var: " << tab.m_attributes[Dbtup::DD].m_no_of_varsize << "]"
      << " ]" << endl;
  return out;
}

NdbOut &operator<<(NdbOut &out, const AttributeDescriptor &off) {
  Uint32 word;
  memcpy(&word, &off, 4);
  return out;
}

NdbOut &operator<<(NdbOut &out, const AttributeOffset &off) {
  Uint32 word;
  memcpy(&word, &off, 4);
  out << "[ offset: " << AttributeOffset::getOffset(word)
      << " nullpos: " << AttributeOffset::getNullFlagPos(word);
  if (AttributeOffset::getCharsetFlag(word))
    out << " charset: %d" << AttributeOffset::getCharsetPos(word);
  out << " ]";
  return out;
}
