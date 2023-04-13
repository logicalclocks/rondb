/*
   Copyright (c) 2003, 2022, Oracle and/or its affiliates.
   Copyright (c) 2021, 2023, Logical Clocks and/or its affiliates.

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

#define DBTUP_C
#define DBTUP_GEN_CPP
#include "util/require.h"
#include <dblqh/Dblqh.hpp>
#include "Dbtup.hpp"
#include <RefConvert.hpp>
#include <ndb_limits.h>
#include <pc.hpp>
#include <AttributeDescriptor.hpp>
#include "AttributeOffset.hpp"
#include <AttributeHeader.hpp>
#include <Interpreter.hpp>
#include <signaldata/FsConf.hpp>
#include <signaldata/FsRef.hpp>
#include <signaldata/FsRemoveReq.hpp>
#include <signaldata/TupCommit.hpp>
#include <signaldata/TupKey.hpp>
#include <signaldata/NodeFailRep.hpp>
#include <signaldata/NodeStateSignalData.hpp>

#include <signaldata/DropTab.hpp>
#include <IntrusiveList.hpp>

#include <EventLogger.hpp>

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DO_TRANSIENT_POOL_STAT 1
//#define DEBUG_AUTOMATIC_MEMORY 1
#endif

#ifdef DEBUG_AUTOMATIC_MEMORY
#define DEB_AUTOMATIC_MEMORY(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_AUTOMATIC_MEMORY(arglist) do { } while (0)
#endif

#define JAM_FILE_ID 420


void Dbtup::initData() 
{
  m_curr_tup = this;
  cownNodeId = getOwnNodeId();
  TablerecPtr tablePtr;
  (void)tablePtr; // hide unused warning
  cnoOfFragoprec = 16;
  cnoOfAlterTabOps = 16;
  c_maxTriggersPerTable = ZDEFAULT_MAX_NO_TRIGGERS_PER_TABLE;
  c_noOfBuildIndexRec = 32;

  c_debug_count = 0;

  // Records with constant sizes
  init_list_sizes();
  cpackedListIndex = 0;
}//Dbtup::initData()

Dbtup::Dbtup(Block_context& ctx,
             Uint32 instanceNumber,
             Uint32 blockNo)
  : SimulatedBlock(blockNo, ctx, instanceNumber),
    c_lqh(0),
    c_backup(0),
    c_tsman(0),
    c_lgman(0),
    c_pgman(0),
    c_acc(0),
    c_tux(0),
    c_suma(0),
    m_reserved_copy_frag(c_scanOpPool),
    m_reserved_copy_frag_lock(c_scanLockPool),
    m_reserved_stored_proc_copy_frag(c_storedProcPool),
    c_extent_hash(c_extent_pool),
    c_storedProcPool(),
    c_buildIndexList(c_buildIndexPool),
    c_undo_buffer(&ctx.m_mm),
    m_pages_allocated(0),
    m_pages_allocated_max(0),
    c_pending_undo_page_hash(c_pending_undo_page_pool),
    f_undo_done(true)
{
  BLOCK_CONSTRUCTOR(Dbtup);

  if (blockNo == DBTUP)
  {
    addRecSignal(GSN_DEBUG_SIG, &Dbtup::execDEBUG_SIG);
    addRecSignal(GSN_CONTINUEB, &Dbtup::execCONTINUEB);
    addRecSignal(GSN_NODE_FAILREP, &Dbtup::execNODE_FAILREP);

    addRecSignal(GSN_DUMP_STATE_ORD, &Dbtup::execDUMP_STATE_ORD);
    addRecSignal(GSN_DBINFO_SCANREQ, &Dbtup::execDBINFO_SCANREQ);
    addRecSignal(GSN_SEND_PACKED, &Dbtup::execSEND_PACKED, true);
    addRecSignal(GSN_STTOR, &Dbtup::execSTTOR);
    addRecSignal(GSN_MEMCHECKREQ, &Dbtup::execMEMCHECKREQ);
    addRecSignal(GSN_TUPSEIZEREQ, &Dbtup::execTUPSEIZEREQ);
    addRecSignal(GSN_STORED_PROCREQ, &Dbtup::execSTORED_PROCREQ); 
    addRecSignal(GSN_CREATE_TAB_REQ, &Dbtup::execCREATE_TAB_REQ);
    addRecSignal(GSN_TUPFRAGREQ, &Dbtup::execTUPFRAGREQ);
    addRecSignal(GSN_TUP_ADD_ATTRREQ, &Dbtup::execTUP_ADD_ATTRREQ);
    addRecSignal(GSN_ALTER_TAB_REQ, &Dbtup::execALTER_TAB_REQ);
    addRecSignal(GSN_TUP_COMMITREQ, &Dbtup::execTUP_COMMITREQ);
    addRecSignal(GSN_TUP_ABORTREQ, &Dbtup::execTUP_ABORTREQ);
    addRecSignal(GSN_NDB_STTOR, &Dbtup::execNDB_STTOR);
    addRecSignal(GSN_READ_CONFIG_REQ, &Dbtup::execREAD_CONFIG_REQ, true);

    // Trigger Signals
    addRecSignal(GSN_CREATE_TRIG_IMPL_REQ, &Dbtup::execCREATE_TRIG_IMPL_REQ);
    addRecSignal(GSN_DROP_TRIG_IMPL_REQ,  &Dbtup::execDROP_TRIG_IMPL_REQ);

    addRecSignal(GSN_DROP_TAB_REQ, &Dbtup::execDROP_TAB_REQ);

    addRecSignal(GSN_TUP_DEALLOCREQ, &Dbtup::execTUP_DEALLOCREQ);
    addRecSignal(GSN_TUP_WRITELOG_REQ, &Dbtup::execTUP_WRITELOG_REQ);

    // Ordered index related
    addRecSignal(GSN_BUILD_INDX_IMPL_REQ, &Dbtup::execBUILD_INDX_IMPL_REQ);
    addRecSignal(GSN_BUILD_INDX_IMPL_REF, &Dbtup::execBUILD_INDX_IMPL_REF);
    addRecSignal(GSN_BUILD_INDX_IMPL_CONF, &Dbtup::execBUILD_INDX_IMPL_CONF);
    addRecSignal(GSN_ALTER_TAB_CONF, &Dbtup::execALTER_TAB_CONF);
    m_max_parallel_index_build = 0;

    // Tup scan
    addRecSignal(GSN_ACC_SCANREQ, &Dbtup::execACC_SCANREQ);
    addRecSignal(GSN_NEXT_SCANREQ, &Dbtup::execNEXT_SCANREQ);
    addRecSignal(GSN_ACC_CHECK_SCAN, &Dbtup::execACC_CHECK_SCAN);
    addRecSignal(GSN_ACCKEYCONF, &Dbtup::execACCKEYCONF);
    addRecSignal(GSN_ACCKEYREF, &Dbtup::execACCKEYREF);
    addRecSignal(GSN_ACC_ABORTCONF, &Dbtup::execACC_ABORTCONF);

    // Drop table
    addRecSignal(GSN_FSREMOVEREF, &Dbtup::execFSREMOVEREF, true);
    addRecSignal(GSN_FSREMOVECONF, &Dbtup::execFSREMOVECONF, true);
    addRecSignal(GSN_FSOPENREF, &Dbtup::execFSOPENREF, true);
    addRecSignal(GSN_FSOPENCONF, &Dbtup::execFSOPENCONF, true);
    addRecSignal(GSN_FSREADREF, &Dbtup::execFSREADREF, true);
    addRecSignal(GSN_FSREADCONF, &Dbtup::execFSREADCONF, true);
    addRecSignal(GSN_FSCLOSEREF, &Dbtup::execFSCLOSEREF, true);
    addRecSignal(GSN_FSCLOSECONF, &Dbtup::execFSCLOSECONF, true);

    addRecSignal(GSN_DROP_FRAG_REQ, &Dbtup::execDROP_FRAG_REQ);
    addRecSignal(GSN_SUB_GCP_COMPLETE_REP, &Dbtup::execSUB_GCP_COMPLETE_REP);

    addRecSignal(GSN_FIRE_TRIG_REQ, &Dbtup::execFIRE_TRIG_REQ);
    m_is_query_block = false;
    m_is_in_query_thread = false;
    m_acc_block = DBACC;
    m_tup_block = DBTUP;
    m_lqh_block = DBLQH;
    m_tux_block = DBTUX;
    m_backup_block = BACKUP;
    m_ldm_instance_used = this;
  }
  else
  {
    ndbrequire(blockNo == DBQTUP);
    m_is_query_block = true;
    m_is_in_query_thread = true;
    m_acc_block = DBQACC;
    m_tup_block = DBQTUP;
    m_lqh_block = DBQLQH;
    m_tux_block = DBQTUX;
    m_backup_block = QBACKUP;
    m_ldm_instance_used = nullptr;
    addRecSignal(GSN_TUP_DEALLOCREQ, &Dbtup::execTUP_DEALLOCREQ);
    addRecSignal(GSN_CONTINUEB, &Dbtup::execCONTINUEB);
    addRecSignal(GSN_DUMP_STATE_ORD, &Dbtup::execDUMP_STATE_ORD);
    addRecSignal(GSN_DBINFO_SCANREQ, &Dbtup::execDBINFO_SCANREQ);
    addRecSignal(GSN_SEND_PACKED, &Dbtup::execSEND_PACKED, true);
    addRecSignal(GSN_STTOR, &Dbtup::execSTTOR);
    addRecSignal(GSN_TUPSEIZEREQ, &Dbtup::execTUPSEIZEREQ);
    addRecSignal(GSN_STORED_PROCREQ, &Dbtup::execSTORED_PROCREQ);
    addRecSignal(GSN_TUP_COMMITREQ, &Dbtup::execTUP_COMMITREQ);
    addRecSignal(GSN_TUP_ABORTREQ, &Dbtup::execTUP_ABORTREQ);
    addRecSignal(GSN_ACC_SCANREQ, &Dbtup::execACC_SCANREQ);
    addRecSignal(GSN_NEXT_SCANREQ, &Dbtup::execNEXT_SCANREQ);
    addRecSignal(GSN_ACC_CHECK_SCAN, &Dbtup::execACC_CHECK_SCAN);
    addRecSignal(GSN_ACCKEYCONF, &Dbtup::execACCKEYCONF);
    addRecSignal(GSN_ACCKEYREF, &Dbtup::execACCKEYREF);
    addRecSignal(GSN_READ_CONFIG_REQ, &Dbtup::execREAD_CONFIG_REQ, true);
  }
  fragoperrec = 0;
  alterTabOperRec = 0;
  hostBuffer = 0;
  tablerec = 0;

  initData();
  CLEAR_ERROR_INSERT_VALUE;

  RSS_OP_COUNTER_INIT(cnoOfFreeFragoprec);
  RSS_OP_COUNTER_INIT(cnoOfAllocatedFragrec);
  RSS_OP_COUNTER_INIT(cnoOfFreeTabDescrRec);
  c_storedProcCountNonAPI = 0;

  {
    CallbackEntry& ce = m_callbackEntry[THE_NULL_CALLBACK];
    ce.m_function = TheNULLCallback.m_callbackFunction;
    ce.m_flags = 0;
  }
  { // 1
    CallbackEntry& ce = m_callbackEntry[DROP_TABLE_LOG_BUFFER_CALLBACK];
    ce.m_function = safe_cast(&Dbtup::drop_table_log_buffer_callback);
    ce.m_flags = 0;
  }
  { // 2
    CallbackEntry& ce = m_callbackEntry[NR_DELETE_LOG_BUFFER_CALLBACK];
    ce.m_function = safe_cast(&Dbtup::nr_delete_log_buffer_callback);
    ce.m_flags = 0;
  }
  { // 3
    CallbackEntry& ce = m_callbackEntry[DISK_PAGE_LOG_BUFFER_CALLBACK];
    ce.m_function = safe_cast(&Dbtup::disk_page_log_buffer_callback);
    ce.m_flags = CALLBACK_ACK;
  }
  {
    CallbackTable& ct = m_callbackTable;
    ct.m_count = COUNT_CALLBACKS;
    ct.m_entry = m_callbackEntry;
    m_callbackTableAddr = &ct;
  }
  c_transient_pools[DBTUP_OPERATION_RECORD_TRANSIENT_POOL_INDEX] =
    &c_operation_pool;
  c_transient_pools[DBTUP_STORED_PROCEDURE_TRANSIENT_POOL_INDEX] =
    &c_storedProcPool;
  c_transient_pools[DBTUP_SCAN_LOCK_TRANSIENT_POOL_INDEX] =
    &c_scanLockPool;
  c_transient_pools[DBTUP_SCAN_OPERATION_TRANSIENT_POOL_INDEX] =
    &c_scanOpPool;
  static_assert(c_transient_pool_count == 4);
  c_transient_pools_shrinking.clear();
}//Dbtup::Dbtup()

Dbtup::~Dbtup() 
{
  /* Free Fragment Copy Procedure info */
  freeCopyProcedure();

  // Records with dynamic sizes
  c_page_pool.clear();
  
  deallocRecord((void **)&fragoperrec,"Fragoperrec",
		sizeof(Fragoperrec),
		cnoOfFragoprec);
  
  deallocRecord((void **)&alterTabOperRec,"AlterTabOperRec",
                sizeof(alterTabOperRec),
                cnoOfAlterTabOps);
  
  deallocRecord((void **)&hostBuffer,"HostBuffer",
		sizeof(HostBuffer), 
		MAX_NODES);
  
  deallocRecord((void **)&tablerec,"Tablerec",
		sizeof(Tablerec), 
		cnoOfTablerec);
}//Dbtup::~Dbtup()

Dbtup::Apply_undo::Apply_undo()
{
  m_in_intermediate_log_record = false;
  m_type = 0;
  m_len = 0;
  m_ptr = 0;
  m_lsn = (Uint64)0;
  m_table_ptr.setNull();
  m_fragment_ptr.setNull();
  m_page_ptr.setNull();
  m_extent_ptr.setNull();
  m_key.setNull();
}

BLOCK_FUNCTIONS(Dbtup)

Uint64 Dbtup::getTransactionMemoryNeed(
    const Uint32 ldm_instance_count,
    const ndb_mgm_configuration_iterator * mgm_cfg)
{
  Uint32 tup_scan_recs = 0;
  Uint32 tup_op_recs = 0;
  Uint32 tup_sp_recs = 0;
  Uint32 tup_scan_lock_recs = 0;

  {
    require(!ndb_mgm_get_int_parameter(mgm_cfg,
                                       CFG_TUP_RESERVED_SCAN_RECORDS,
                                       &tup_scan_recs));
    require(!ndb_mgm_get_int_parameter(mgm_cfg,
                                       CFG_LDM_RESERVED_OPERATIONS,
                                       &tup_op_recs));
    tup_sp_recs = tup_scan_recs;
    tup_scan_lock_recs = 1000;
  }
  Uint64 scan_op_byte_count = 0;
  scan_op_byte_count += ScanOp_pool::getMemoryNeed(tup_scan_recs + 1);
  scan_op_byte_count *= ldm_instance_count;

  Uint64 op_byte_count = 0;
  op_byte_count += Operationrec_pool::getMemoryNeed(tup_op_recs);
  op_byte_count *= ldm_instance_count;

  Uint64 sp_byte_count = 0;
  sp_byte_count += StoredProc_pool::getMemoryNeed(tup_sp_recs);
  sp_byte_count *= ldm_instance_count;

  Uint64 scan_lock_byte_count = 0;
  scan_lock_byte_count += ScanLock_pool::getMemoryNeed(tup_scan_lock_recs);
  scan_lock_byte_count *= ldm_instance_count;

  return (op_byte_count +
          sp_byte_count +
          scan_lock_byte_count +
          scan_op_byte_count);
}

void Dbtup::execCONTINUEB(Signal* signal) 
{
  jamEntry();
  Uint32 actionType = signal->theData[0];
  Uint32 dataPtr = signal->theData[1];

  switch (actionType) {
  case ZTUP_REPORT_COMMIT_PERFORMED:
  {
    jam();
    continue_report_commit_performed(signal, dataPtr);
    return;
  }
  case ZTUP_SHRINK_TRANSIENT_POOLS:
  {
    jam();
    Uint32 pool_index = signal->theData[1];
    ndbassert(signal->getLength() == 2);
    shrinkTransientPools(pool_index);
    return;
  }
#if (defined(VM_TRACE) || \
     defined(ERROR_INSERT)) && \
    defined(DO_TRANSIENT_POOL_STAT)

  case ZTUP_TRANSIENT_POOL_STAT:
  {
    for (Uint32 pool_index = 0;
         pool_index < c_transient_pool_count;
         pool_index++)
    {
      g_eventLogger->info(
        "DBTUP %u: Transient slot pool %u %p: Entry size %u:"
       " Free %u: Used %u: Used high %u: Size %u: For shrink %u",
       instance(),
       pool_index,
       c_transient_pools[pool_index],
       c_transient_pools[pool_index]->getEntrySize(),
       c_transient_pools[pool_index]->getNoOfFree(),
       c_transient_pools[pool_index]->getUsed(),
       c_transient_pools[pool_index]->getUsedHi(),
       c_transient_pools[pool_index]->getSize(),
       c_transient_pools_shrinking.get(pool_index));
    }
    sendSignalWithDelay(reference(), GSN_CONTINUEB, signal, 5000, 1);
    break;
  }
#endif
  case ZINITIALISE_RECORDS:
    jam();
    initialiseRecordsLab(signal, dataPtr, 
			 signal->theData[2], signal->theData[3]);
    break;
  case ZREL_FRAG:
    jam();
    releaseFragment(signal, dataPtr, signal->theData[2]);
    break;
  case ZBUILD_INDEX:
    jam();
    buildIndex(signal, dataPtr);
    break;
  case ZTUP_SCAN:
    jam();
    {
      ScanOpPtr scanPtr;
      scanPtr.i = dataPtr;
      ndbrequire(c_scanOpPool.getValidPtr(scanPtr));
      c_lqh->checkLcpStopBlockedLab(signal, scanPtr.p->m_userPtr);
    }
    return;
  case ZFREE_EXTENT:
  {
    jam();
    TablerecPtr tabPtr;
    FragrecordPtr fragPtr;
    ndbrequire(get_fragment_record(tabPtr,
                                   fragPtr,
                                   dataPtr,
                                   signal->theData[2]));
    drop_fragment_free_extent(signal, tabPtr, fragPtr, signal->theData[3]);
    return;
  }
  case ZUNMAP_PAGES:
  {
    jam();
    TablerecPtr tabPtr;
    FragrecordPtr fragPtr;
    ndbrequire(get_fragment_record(tabPtr,
                                   fragPtr,
                                   dataPtr,
                                   signal->theData[2]));
    drop_fragment_unmap_pages(signal, tabPtr, fragPtr, signal->theData[3]);
    return;
  }
  case ZFREE_VAR_PAGES:
  {
    jam();
    TablerecPtr tabPtr;
    FragrecordPtr fragPtr;
    ndbrequire(get_fragment_record(tabPtr,
                                   fragPtr,
                                   dataPtr,
                                   signal->theData[2]));
    drop_fragment_free_var_pages(signal, tabPtr, fragPtr);
    return;
  }
  case ZFREE_PAGES:
  {
    jam();
    TablerecPtr tabPtr;
    FragrecordPtr fragPtr;
    ndbrequire(get_fragment_record(tabPtr,
                                   fragPtr,
                                   dataPtr,
                                   signal->theData[2]));
    drop_fragment_free_pages(signal, tabPtr, fragPtr);
    return;
  }
  case ZREBUILD_FREE_PAGE_LIST:
  {
    jam();
    rebuild_page_free_list(signal);
    return;
  }
  case ZDISK_RESTART_UNDO:
  {
    jam();
    if (!assembleFragments(signal)) {
      jam();
      return;
    }
    Uint32 type = signal->theData[1];
    Uint32 len = signal->theData[2];
    Uint64 lsn_hi = signal->theData[3];
    Uint64 lsn_lo = signal->theData[4];
    Uint64 lsn = (lsn_hi << 32) | lsn_lo;
    SectionHandle handle(this, signal);
    ndbrequire(handle.m_cnt == 1);
    SegmentedSectionPtr ssptr;
    ndbrequire(handle.getSection(ssptr, 0));
    ndbrequire(ssptr.sz <= NDB_ARRAY_SIZE(f_undo.m_data));
    ::copy(f_undo.m_data, ssptr);
    releaseSections(handle);
    disk_restart_undo(signal,
                      lsn,
                      type,
                      f_undo.m_data,
                      len);
    return;
  }

  default:
    ndbabort();
  }//switch
}//Dbtup::execTUP_CONTINUEB()

/* **************************************************************** */
/* ---------------------------------------------------------------- */
/* ------------------- SYSTEM RESTART MODULE ---------------------- */
/* ---------------------------------------------------------------- */
/* **************************************************************** */
void Dbtup::execSTTOR(Signal* signal) 
{
  jamEntry();
  Uint32 startPhase = signal->theData[1];
  Uint32 sigKey = signal->theData[6];
  switch (startPhase) {
  case ZSTARTPHASE1:
    jam();
    c_started = false;
    if (m_is_query_block)
    {
      ndbrequire((c_tux = (Dbtux*)globalData.getBlock(DBQTUX,
                                                      instance())) != 0);
      ndbrequire((c_acc = (Dbacc*)globalData.getBlock(DBQACC,
                                                      instance())) != 0);
      ndbrequire((c_lqh = (Dblqh*)globalData.getBlock(DBQLQH,
                                                      instance())) != 0);
      ndbrequire((c_backup =
        (Backup*)globalData.getBlock(QBACKUP, instance())) != 0);
    }
    else
    {
      ndbrequire((c_tux = (Dbtux*)globalData.getBlock(DBTUX,
                                                      instance())) != 0);
      ndbrequire((c_acc = (Dbacc*)globalData.getBlock(DBACC,
                                                      instance())) != 0);
      ndbrequire((c_lqh = (Dblqh*)globalData.getBlock(DBLQH,
                                                      instance())) != 0);
      ndbrequire((c_backup =
        (Backup*)globalData.getBlock(BACKUP, instance())) != 0);
    }
    ndbrequire((c_suma = (Suma*)globalData.getBlock(SUMA)) != 0);
    ndbrequire((c_tsman = (Tsman*)globalData.getBlock(TSMAN)) != 0);
    ndbrequire((c_lgman = (Lgman*)globalData.getBlock(LGMAN)) != 0);
    ndbrequire((c_pgman =
                (Pgman*)globalData.getBlock(PGMAN, instance())) != 0);
    cownref = reference();
    break;
  case 3:
  {
#if (defined(VM_TRACE) || \
     defined(ERROR_INSERT)) && \
    defined(DO_TRANSIENT_POOL_STAT)
    /* Start reporting statistics for transient pools */
    signal->theData[0] = ZTUP_TRANSIENT_POOL_STAT;
    sendSignal(reference(), GSN_CONTINUEB, signal, 1, JBB);
#endif
    break;
  }
  case 8:
  {
    c_restart_allow_use_spare = false;
    break;
  }
  case 50:
    c_started = true;
    break;
  default:
    jam();
    break;
  }//switch
  if (m_is_query_block)
  {
    jam();
    signal->theData[0] = sigKey;
    signal->theData[1] = 3;
    signal->theData[2] = 2;
    signal->theData[3] = ZSTARTPHASE1;
    signal->theData[4] = 3;
    signal->theData[5] = 8;
    signal->theData[6] = 50;
    signal->theData[7] = 255;
    sendSignal(DBQTUP_REF, GSN_STTORRY, signal, 8, JBB);
  }
  else
  {
    jam();
    signal->theData[0] = sigKey;
    signal->theData[1] = 3;
    signal->theData[2] = 2;
    signal->theData[3] = ZSTARTPHASE1;
    signal->theData[4] = 3;
    signal->theData[5] = 8;
    signal->theData[6] = 50;
    signal->theData[7] = 255;
    BlockReference cntrRef = !isNdbMtLqh() ? NDBCNTR_REF : DBTUP_REF;
    sendSignal(cntrRef, GSN_STTORRY, signal, 8, JBB);
  }
}//Dbtup::execSTTOR()

/************************************************************************************************/
// SIZE_ALTREP INITIALIZE DATA STRUCTURES, FILES AND DS VARIABLES, GET READY FOR EXTERNAL 
// CONNECTIONS.
/************************************************************************************************/
void Dbtup::execREAD_CONFIG_REQ(Signal* signal) 
{
  const ReadConfigReq * req = (ReadConfigReq*)signal->getDataPtr();
  Uint32 ref = req->senderRef;
  Uint32 senderData = req->senderData;
  ndbrequire(req->noOfParameters == 0);
  
  jamEntry();

  const ndb_mgm_configuration_iterator * p = 
    m_ctx.m_config.getOwnConfigIterator();
  ndbrequire(p != 0);
  
  ndbrequire(!ndb_mgm_get_int_parameter(p, CFG_TUP_TABLE, &cnoOfTablerec));

  initRecords(p);

  // Allocate fragment copy procedure
  allocCopyProcedure();

  if (m_is_query_block)
  {
    jam();
    c_noOfBuildIndexRec = 0;
  }
  c_buildIndexPool.setSize(c_noOfBuildIndexRec);

  c_extent_hash.setSize(8192); // 4k

  c_pending_undo_page_hash.setSize(MAX_PENDING_UNDO_RECORDS);
  
  Pool_context pc;
  pc.m_block = this;
  c_triggerPool.init(RT_DBTUP_TRIGGER_DATA, pc);
  cnoOfAllocatedTriggerRec = 0;
  cnoOfMaxAllocatedTriggerRec = 0;
  c_page_request_pool.wo_pool_init(RT_DBTUP_PAGE_REQUEST, pc);
  c_apply_undo_pool.init(RT_DBTUP_UNDO, pc);
  c_pending_undo_page_pool.init(RT_DBTUP_UNDO, pc);

  c_extent_pool.init(RT_DBTUP_EXTENT_INFO, pc);
  if (!m_is_query_block)
  {
    NdbMutex_Init(&c_page_map_pool_mutex);
    c_page_map_pool.init(&c_page_map_pool_mutex, RT_DBTUP_PAGE_MAP, pc);
    c_page_map_pool_ptr = &c_page_map_pool;
  }
  else
  {
    c_page_map_pool_ptr = 0;
  }

  c_fragment_pool.init(RT_DBTUP_FRAGMENT, pc);

  /* read ahead for disk scan can not be more that disk page buffer */
  {
    Uint64 page_cache_size = globalData.theDiskPageBufferMemory;
    page_cache_size = (page_cache_size  + GLOBAL_PAGE_SIZE - 1) /
                       GLOBAL_PAGE_SIZE; // in pages
    // never read ahead more than 32 pages
    if (page_cache_size > 32)
      m_max_page_read_ahead = 32;
    else
      m_max_page_read_ahead = (Uint32)page_cache_size;
  }


  ScanOpPtr lcp;
  ndbrequire(c_scanOpPool.seize(lcp));
  c_lcp_scan_op = lcp.i;

  for (Uint32 i = 0; i < ZMAX_PARALLEL_COPY_FRAGMENT_OPS; i++)
  {
    ScanOpPtr copy_frag;
    ndbrequire(c_scanOpPool.seize(copy_frag));
    m_reserved_copy_frag.addFirst(copy_frag);
    copy_frag.p->m_state = ScanOp::First;
    copy_frag.p->m_bits = 0;
  }

  czero = 0;
  cminusOne = czero - 1;
  clastBitMask = 1;
  clastBitMask = clastBitMask << 31;

  ndb_mgm_get_int_parameter(p, CFG_DB_MT_BUILD_INDEX,
                            &m_max_parallel_index_build);

  if (globalData.ndbMtLqhWorkers > 1)
  {
    /**
     * Divide by LQH threads
     */
    Uint32 val = m_max_parallel_index_build;
    val = (val + instance() - 1) / globalData.ndbMtLqhWorkers;
    m_max_parallel_index_build = val;
  }
  
  initialiseRecordsLab(signal, 0, ref, senderData);

  {
    Uint32 val = 0;
    ndb_mgm_get_int_parameter(p, CFG_DB_CRASH_ON_CORRUPTED_TUPLE,
                              &val);
    c_crashOnCorruptedTuple = val ? true : false;
  }
  /**
   * Set up read buffer used by Drop Table
   */
  NewVARIABLE *bat = allocateBat(1);
  bat[0].WA = &m_read_ctl_file_data[0];
  bat[0].nrr = (BackupFormat::LCP_CTL_FILE_BUFFER_SIZE_IN_WORDS * 4);
}

void Dbtup::initRecords(const ndb_mgm_configuration_iterator *mgm_cfg) 
{
  unsigned i;
  const ndb_mgm_configuration_iterator * p = 
    m_ctx.m_config.getOwnConfigIterator();
  ndbrequire(p != 0);

#if defined(USE_INIT_GLOBAL_VARIABLES)
  {
    void* tmp[] =
    {
      &prepare_fragptr,
      &prepare_tabptr,
      &prepare_oper_ptr,
      &prepare_pageptr,
      &m_curr_tabptr,
      &m_curr_fragptr,
    };
    init_global_ptrs(tmp, sizeof(tmp)/sizeof(tmp[0]));
  }
  {
    void * tmp[] =
    {
      &prepare_page_idx,
      &prepare_page_no,
    };
    init_global_uint32(tmp, sizeof(tmp)/sizeof(tmp[0]));
  }
  {
    void * tmp[] =
    {
      &prepare_tuple_ptr,
    };
    init_global_uint32_ptrs(tmp, sizeof(tmp)/sizeof(tmp[0]));
  }
#endif
  // Records with dynamic sizes
  void* ptr = m_ctx.m_mm.get_memroot();
  c_page_pool.set((Page*)ptr, (Uint32)~0);
  c_restart_allow_use_spare = true;

  if (m_is_query_block)
  {
    cnoOfFragoprec = 1;
    cnoOfAlterTabOps = 0;
    cnoOfTablerec = 0;
  }
  fragoperrec = (Fragoperrec*)allocRecord("Fragoperrec",
					  sizeof(Fragoperrec),
					  cnoOfFragoprec);

  alterTabOperRec = (AlterTabOperation*)allocRecord("AlterTabOperation",
                                                    sizeof(AlterTabOperation),
                                                    cnoOfAlterTabOps);

  hostBuffer = (HostBuffer*)allocRecord("HostBuffer",
					sizeof(HostBuffer), 
					MAX_NODES);

  tablerec = (Tablerec*)allocRecord("Tablerec",
				    sizeof(Tablerec), 
				    cnoOfTablerec);

  for (i = 0; i<cnoOfTablerec; i++) {
    void * p = &tablerec[i];
    new (p) Tablerec(c_triggerPool);
  }

  Pool_context pc;
  pc.m_block = this;

  Uint32 reserveOpRecs = 1;
  ndbrequire(!ndb_mgm_get_int_parameter(mgm_cfg,
                             CFG_LDM_RESERVED_OPERATIONS,
                             &reserveOpRecs));
  if (m_is_query_block)
  {
    reserveOpRecs = 200;
  }
  c_operation_pool.init(
    Operationrec::TYPE_ID,
    pc,
    reserveOpRecs,
    UINT32_MAX);
  while (c_operation_pool.startup())
  {
    refresh_watch_dog();
  }

  Uint32 reserveSpRecs = 200;
  ndbrequire(!ndb_mgm_get_int_parameter(mgm_cfg,
                             CFG_TUP_RESERVED_SCAN_RECORDS,
                             &reserveSpRecs));
  if (m_is_query_block)
  {
    reserveSpRecs = 1;
  }
  c_storedProcPool.init(
    storedProc::TYPE_ID,
    pc,
    reserveSpRecs,
    UINT32_MAX);
  while (c_storedProcPool.startup())
  {
    refresh_watch_dog();
  }

  Uint32 tup_scan_lock_recs = 1000;
  c_freeScanLock = RNIL;
  if (m_is_query_block)
  {
    tup_scan_lock_recs = 1;
  }
  c_scanLockPool.init(
    ScanLock::TYPE_ID,
    pc,
    tup_scan_lock_recs,
    UINT32_MAX);
  while (c_scanLockPool.startup())
  {
    refresh_watch_dog();
  }
  for (Uint32 i = 0; i < ZMAX_PARALLEL_COPY_FRAGMENT_OPS; i++)
  {
    ScanLockPtr lockPtr;
    ndbrequire(c_scanLockPool.seize(lockPtr));
    lockPtr.p->m_accLockOp = RNIL;
    lockPtr.p->prevList = RNIL;
    lockPtr.p->nextList = RNIL;
    lockPtr.p->m_reserved = 1;
    m_reserved_copy_frag_lock.addFirst(lockPtr);
  }

  c_scanOpPool.init(
    ScanOp::TYPE_ID,
    pc,
    reserveSpRecs + 1,
    UINT32_MAX);
  while (c_scanOpPool.startup())
  {
    refresh_watch_dog();
  }
}//Dbtup::initRecords()

void Dbtup::initialiseRecordsLab(Signal* signal, Uint32 switchData,
				 Uint32 retRef, Uint32 retData) 
{
  switch (switchData) {
  case 0:
    jam();
    initializeHostBuffer();
    break;
  case 1:
    jam();
    initializePage();
    break;
  case 2:
    jam();
    initializeTablerec();
    break;
  case 3:
    jam();
    break;
  case 4:
    jam();
    initializeFragoperrec();
    break;
  case 5:
    jam();
    break;
  case 6:
    jam();
    initializeAlterTabOperation();
    break;
  case 7:
    jam();

    {
      ReadConfigConf * conf = (ReadConfigConf*)signal->getDataPtrSend();
      conf->senderRef = reference();
      conf->senderData = retData;
      sendSignal(retRef, GSN_READ_CONFIG_CONF, signal, 
		 ReadConfigConf::SignalLength, JBB);
    }
    return;
  default:
    ndbabort();
  }//switch
  signal->theData[0] = ZINITIALISE_RECORDS;
  signal->theData[1] = switchData + 1;
  signal->theData[2] = retRef;
  signal->theData[3] = retData;
  sendSignal(reference(), GSN_CONTINUEB, signal, 4, JBB);
  return;
}//Dbtup::initialiseRecordsLab()

void Dbtup::execNDB_STTOR(Signal* signal) 
{
  jamEntry();
  cndbcntrRef = signal->theData[0];
  Uint32 startPhase = signal->theData[2];
  switch (startPhase) {
  case ZSTARTPHASE1:
    jam();
    ndbassert(!m_is_query_block);
    initializeDefaultValuesFrag();
    break;
  case ZSTARTPHASE2:
    jam();
    break;
  case ZSTARTPHASE3:
    jam();
    break;
  case ZSTARTPHASE4:
    jam();
    break;
  case ZSTARTPHASE6:
    jam();
    break;
  default:
    jam();
    break;
  }//switch
  signal->theData[0] = cownref;
  BlockReference cntrRef = !isNdbMtLqh() ? NDBCNTR_REF : DBTUP_REF;
  sendSignal(cntrRef, GSN_NDB_STTORRY, signal, 1, JBB);
}//Dbtup::execNDB_STTOR()

void Dbtup::initializeDefaultValuesFrag()
{
  /* Grab and initialize a fragment record for storing default
   * values for the table fragments held by this TUP instance
   */
  ndbrequire(seizeFragrecord(DefaultValuesFragment));
  DefaultValuesFragment.p->fragStatus = Fragrecord::FS_ONLINE;
  DefaultValuesFragment.p->m_undo_complete= 0;
  DefaultValuesFragment.p->m_lcp_scan_op = RNIL;
  DefaultValuesFragment.p->noOfPages = 0;
  DefaultValuesFragment.p->noOfVarPages = 0;
  DefaultValuesFragment.p->m_varWordsFree = 0;
  DefaultValuesFragment.p->m_max_page_cnt = 0;
  DefaultValuesFragment.p->m_free_page_id_list = FREE_PAGE_RNIL;
  ndbrequire(DefaultValuesFragment.p->m_page_map.isEmpty());
  DefaultValuesFragment.p->m_restore_lcp_id = RNIL;
  for (Uint32 i = 0; i<MAX_FREE_LIST+1; i++)
    ndbrequire(DefaultValuesFragment.p->free_var_page_array[i].isEmpty());

  DefaultValuesFragment.p->m_logfile_group_id = RNIL;

  return;
}

void Dbtup::initializeFragoperrec() 
{
  FragoperrecPtr fragoperPtr;
  for (fragoperPtr.i = 0; fragoperPtr.i < cnoOfFragoprec; fragoperPtr.i++) {
    ptrAss(fragoperPtr, fragoperrec);
    fragoperPtr.p->nextFragoprec = fragoperPtr.i + 1;
  }//for
  fragoperPtr.i = cnoOfFragoprec - 1;
  ptrAss(fragoperPtr, fragoperrec);
  fragoperPtr.p->nextFragoprec = RNIL;
  cfirstfreeFragopr = 0;
}//Dbtup::initializeFragoperrec()

void Dbtup::initializeAlterTabOperation()
{
  if (m_is_query_block)
  {
    cfirstfreeAlterTabOp = RNIL;
    return;
  }
  AlterTabOperationPtr regAlterTabOpPtr;
  for (regAlterTabOpPtr.i= 0;
       regAlterTabOpPtr.i<cnoOfAlterTabOps;
       regAlterTabOpPtr.i++)
  {
    refresh_watch_dog();
    ptrAss(regAlterTabOpPtr, alterTabOperRec);
    new (regAlterTabOpPtr.p) AlterTabOperation();
    regAlterTabOpPtr.p->nextAlterTabOp= regAlterTabOpPtr.i+1;
  }
  regAlterTabOpPtr.i= cnoOfAlterTabOps-1;
  ptrAss(regAlterTabOpPtr, alterTabOperRec);
  regAlterTabOpPtr.p->nextAlterTabOp= RNIL;
  cfirstfreeAlterTabOp= 0;
}

void Dbtup::initializeHostBuffer() 
{
  Uint32 hostId;
  cpackedListIndex = 0;
  for (hostId = 0; hostId < MAX_NODES; hostId++) {
    hostBuffer[hostId].inPackedList = false;
    hostBuffer[hostId].noOfPacketsTA = 0;
    hostBuffer[hostId].packetLenTA = 0;
  }//for
}//Dbtup::initializeHostBuffer()


void Dbtup::initializeTablerec() 
{
  TablerecPtr regTabPtr;
  jam();
  jamData(cnoOfTablerec);
  for (regTabPtr.i = 0; regTabPtr.i < cnoOfTablerec; regTabPtr.i++) {
    refresh_watch_dog();
    ptrAss(regTabPtr, tablerec);
    initTab(regTabPtr.p);
  }//for
}//Dbtup::initializeTablerec()

void
Dbtup::initTab(Tablerec* const regTabPtr)
{
  regTabPtr->readFunctionArray = nullptr;
  regTabPtr->updateFunctionArray = nullptr;
  regTabPtr->charsetArray = nullptr;
  regTabPtr->tabDescriptor = nullptr;
  regTabPtr->readKeyArray = nullptr;
  regTabPtr->dynTabDescriptor[MM] = nullptr;
  regTabPtr->dynTabDescriptor[DD] = nullptr;
  regTabPtr->dynFixSizeMask[MM] = nullptr;
  regTabPtr->dynVarSizeMask[MM] = nullptr;
  regTabPtr->dynFixSizeMask[DD] = nullptr;
  regTabPtr->dynVarSizeMask[DD] = nullptr;

  regTabPtr->m_bits = 0;
  regTabPtr->total_rec_size = 0;
  regTabPtr->m_no_of_extra_columns = 0;
  regTabPtr->m_dyn_null_bits[MM] = 0;
  regTabPtr->m_dyn_null_bits[DD] = 0;
  regTabPtr->noOfKeyAttr = 0;
  regTabPtr->noOfCharsets = 0;
  regTabPtr->m_no_of_real_disk_attributes = 0;
  regTabPtr->m_no_of_disk_attributes = 0;
  regTabPtr->m_no_of_attributes = 0;
  memset(&regTabPtr->m_attributes, 0, sizeof(regTabPtr->m_attributes));
  memset(&regTabPtr->m_offsets, 0, sizeof(regTabPtr->m_offsets));
  regTabPtr->m_allow_use_spare = false;

  regTabPtr->m_dropTable.tabUserPtr = RNIL;
  regTabPtr->m_dropTable.tabUserRef = 0;
  regTabPtr->m_dropTable.m_fragPtrI = RNIL64;
  regTabPtr->m_dropTable.m_outstanding_ops = 0;
  regTabPtr->m_dropTable.m_filePointer = RNIL;
  regTabPtr->m_dropTable.m_firstFileId = ZNIL;
  regTabPtr->m_dropTable.m_lastFileId = ZNIL;
  regTabPtr->m_dropTable.m_numDataFiles = ZNIL;
  regTabPtr->m_dropTable.m_file_type = Z8NIL;
  regTabPtr->m_dropTable.m_lcpno = Z8NIL;

  regTabPtr->m_createTable.m_fragOpPtrI = RNIL;
  regTabPtr->m_createTable.defValSectionI = RNIL;
  regTabPtr->m_createTable.defValLocation.setNull();

  regTabPtr->m_reorg_suma_filter.m_gci_hi = Uint32(~0);

  regTabPtr->tableStatus = NOT_DEFINED;
  regTabPtr->m_default_value_location.setNull();

  // Clear trigger data
  if (!regTabPtr->afterInsertTriggers.isEmpty())
    while (regTabPtr->afterInsertTriggers.releaseFirst());
  if (!regTabPtr->afterDeleteTriggers.isEmpty())
    while (regTabPtr->afterDeleteTriggers.releaseFirst());
  if (!regTabPtr->afterUpdateTriggers.isEmpty())
    while (regTabPtr->afterUpdateTriggers.releaseFirst());
  if (!regTabPtr->subscriptionInsertTriggers.isEmpty())
    while (regTabPtr->subscriptionInsertTriggers.releaseFirst());
  if (!regTabPtr->subscriptionDeleteTriggers.isEmpty())
    while (regTabPtr->subscriptionDeleteTriggers.releaseFirst());
  if (!regTabPtr->subscriptionUpdateTriggers.isEmpty())
    while (regTabPtr->subscriptionUpdateTriggers.releaseFirst());
  if (!regTabPtr->constraintUpdateTriggers.isEmpty())
    while (regTabPtr->constraintUpdateTriggers.releaseFirst());
  if (!regTabPtr->tuxCustomTriggers.isEmpty())
    while (regTabPtr->tuxCustomTriggers.releaseFirst());
}//Dbtup::initTab()

/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* --------------- CONNECT/DISCONNECT MODULE ---------------------- */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
void Dbtup::execTUPSEIZEREQ(Signal* signal)
{
  OperationrecPtr regOperPtr;
  jamEntry();
  Uint32 userPtr = signal->theData[0];
  BlockReference userRef = signal->theData[1];
  if (!c_operation_pool.seize(regOperPtr))
  {
    jam();
    signal->theData[0] = userPtr;
    signal->theData[1] = ZGET_OPREC_ERROR;
    sendSignal(userRef, GSN_TUPSEIZEREF, signal, 2, JBB);
    return;
  }//if
  initOpConnection(regOperPtr.p);
  regOperPtr.p->userpointer = userPtr;
  signal->theData[0] = regOperPtr.p->userpointer;
  signal->theData[1] = regOperPtr.i;
  sendSignal(userRef, GSN_TUPSEIZECONF, signal, 2, JBB);
  return;
}//Dbtup::execTUPSEIZEREQ()

Dbtup::Operationrec*
Dbtup::get_operation_ptr(Uint32 i)
{
  OperationrecPtr opPtr;
  opPtr.i = i;
  require(c_operation_pool.getValidPtr(opPtr));
  return opPtr.p;
}

bool Dbtup::seize_op_rec(Uint32 userPtr,
                         BlockReference ref,
                         Uint32 &i_val,
                         Dbtup::Operationrec **opPtrP)
{
  /* Cannot use jam here, called from other thread */
  OperationrecPtr opPtr;
  (void)ref;
  if (unlikely(!c_operation_pool.seize(opPtr)))
  {
    return false;
  }
  opPtr.p->userpointer = userPtr;
  initOpConnection(opPtr.p);
  i_val = opPtr.i;
  *opPtrP = opPtr.p;
  return true;
}

void Dbtup::releaseFragrec(FragrecordPtr regFragPtr) 
{
  for (Uint32 i = 0; i < NUM_TUP_FRAGMENT_MUTEXES; i++)
  {
    NdbMutex_Deinit(&regFragPtr.p->tup_frag_mutex[i]);
  }
  NdbMutex_Deinit(&regFragPtr.p->tup_frag_page_map_mutex);
  RSS_OP_FREE(cnoOfAllocatedFragrec);
  c_fragment_pool.release(regFragPtr);
}//Dbtup::releaseFragrec()


void Dbtup::execNODE_FAILREP(Signal* signal)
{
  jamEntry();
  NodeFailRep * rep = (NodeFailRep*)signal->getDataPtr();
  if(signal->getLength() == NodeFailRep::SignalLength)
  {
    ndbrequire(signal->getNoOfSections() == 1);
    ndbrequire(ndbd_send_node_bitmask_in_section(
        getNodeInfo(refToNode(signal->getSendersBlockRef())).m_version));
    SegmentedSectionPtr ptr;
    SectionHandle handle(this, signal);
    ndbrequire(handle.getSection(ptr, 0));
    memset(rep->theNodes, 0, sizeof(rep->theNodes));
    copy(rep->theNodes, ptr);
    releaseSections(handle);
  }
  else
  {
    memset(rep->theNodes + NdbNodeBitmask48::Size,
           0,
           _NDB_NBM_DIFF_BYTES);
  }
  NdbNodeBitmask failed; 
  failed.assign(NdbNodeBitmask::Size, rep->theNodes);

  /* Block level cleanup */
  for(unsigned i = 1; i < MAX_NDB_NODES; i++) {
    jam();
    if(failed.get(i)) {
      jam();
      Uint32 elementsCleaned = simBlockNodeFailure(signal, i); // No callback
      ndbassert(elementsCleaned == 0); // No distributed fragmented signals
      (void) elementsCleaned; // Remove compiler warning
    }//if
  }//for
}

void
Dbtup::sendPoolShrink(const Uint32 pool_index)
{
  const bool need_send = c_transient_pools_shrinking.get(pool_index) == 0;
  c_transient_pools_shrinking.set(pool_index);
  if (need_send)
  {
    Signal25 signal[1] = {};
    signal->theData[0] = ZTUP_SHRINK_TRANSIENT_POOLS;
    signal->theData[1] = pool_index;
    sendSignal(reference(), GSN_CONTINUEB, signal, 2, JBB);
  }
}

void
Dbtup::shrinkTransientPools(Uint32 pool_index)
{
  ndbrequire(pool_index < c_transient_pool_count);
  ndbrequire(c_transient_pools_shrinking.get(pool_index));
  if (c_transient_pools[pool_index]->rearrange_free_list_and_shrink())
  {
    sendPoolShrink(pool_index);
  }
  else
  {
    c_transient_pools_shrinking.clear(pool_index);
  }
}
