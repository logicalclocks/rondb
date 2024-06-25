/*
   Copyright (c) 2003, 2023, Oracle and/or its affiliates.
   Copyright (c) 2021, 2024, Hopsworks and/or its affiliates.

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
#include <dblqh/Dblqh.hpp>
#include <cstring>
#include "Dbtup.hpp"
#include <RefConvert.hpp>
#include <ndb_limits.h>
#include <pc.hpp>
#include <AttributeDescriptor.hpp>
#include "AttributeOffset.hpp"
#include <AttributeHeader.hpp>
#include <Interpreter.hpp>
#include <signaldata/TupKey.hpp>
#include <signaldata/AttrInfo.hpp>
#include <signaldata/TuxMaint.hpp>
#include <signaldata/ScanFrag.hpp>
#include <signaldata/TransIdAI.hpp>
#include <signaldata/LqhKey.hpp>
#include <NdbSqlUtil.hpp>
#include <Checksum.hpp>
#include <portlib/ndb_prefetch.h>
#include "../dblqh/Dblqh.hpp"

#define JAM_FILE_ID 422

#define TUP_NO_TUPLE_FOUND 626
#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_LCP 1
//#define DEBUG_REORG 1
//#define DEBUG_DELETE 1
//#define DEBUG_DELETE_NR 1
//#define DEBUG_LCP_LGMAN 1
//#define DEBUG_LCP_SKIP_DELETE 1
//#define DEBUG_DISK 1
//#define DEBUG_ELEM_COUNT 1
#endif

#ifdef DEBUG_REORG
#define DEB_REORG(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_REORG(arglist) do { } while (0)
#endif

#ifdef DEBUG_ELEM_COUNT
#define DEB_ELEM_COUNT(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_ELEM_COUNT(arglist) do { } while (0)
#endif

#ifdef DEBUG_DISK
#define DEB_DISK(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_DISK(arglist) do { } while (0)
#endif

#ifdef DEBUG_LCP
#define DEB_LCP(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_LCP(arglist) do { } while (0)
#endif

#ifdef DEBUG_DELETE
#define DEB_DELETE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_DELETE(arglist) do { } while (0)
#endif

#ifdef DEBUG_LCP_SKIP_DELETE
#define DEB_LCP_SKIP_DELETE(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_LCP_SKIP_DELETE(arglist) do { } while (0)
#endif

#ifdef DEBUG_DELETE_NR
#define DEB_DELETE_NR(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_DELETE_NR(arglist) do { } while (0)
#endif

#ifdef DEBUG_LCP_LGMAN
#define DEB_LCP_LGMAN(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_LCP_LGMAN(arglist) do { } while (0)
#endif

#define TRACE_INTERPRETER

/* For debugging */
static void
dump_hex(const Uint32 *p, Uint32 len)
{
  if(len > 2560)
    len= 160;
  if(len==0)
    return;
  for(;;)
  {
    if(len>=4)
      g_eventLogger->info("%8p %08X %08X %08X %08X", p, p[0], p[1], p[2], p[3]);
    else if(len>=3)
      g_eventLogger->info("%8p %08X %08X %08X", p, p[0], p[1], p[2]);
    else if(len>=2)
      g_eventLogger->info("%8p %08X %08X", p, p[0], p[1]);
    else
      g_eventLogger->info("%8p %08X", p, p[0]);
    if(len <= 4)
      break;
    len-= 4;
    p+= 4;
  }
}

static inline
void
zero32(Uint8* dstPtr, const Uint32 len)
{
  Uint32 odd = len & 3;
  if (odd != 0)
  {
    Uint32 aligned = len & ~3;
    Uint8* dst = dstPtr+aligned;
    switch(odd){     /* odd is: {1..3} */
    case 1:
      dst[1] = 0;
      [[fallthrough]];
    case 2:
      dst[2] = 0;
      [[fallthrough]];
    default:         /* Known to be odd==3 */
      dst[3] = 0;
    }
  }
} 

void Dbtup::copyAttrinfo(Uint32 expectedLen,
                         Uint32 attrInfoIVal)
{
  ndbassert( expectedLen > 0 || attrInfoIVal == RNIL );

  if (expectedLen > 0)
  {
    ndbassert(attrInfoIVal != RNIL);

    /* Check length in section is as we expect */
    SegmentedSectionPtr sectionPtr;
    getSection(sectionPtr, attrInfoIVal);

    ndbrequire(sectionPtr.sz == expectedLen);
    ndbrequire(sectionPtr.sz < ZATTR_BUFFER_SIZE);

    /* Copy attrInfo data into linear buffer */
    // TODO : Consider operating TUP out of first segment where
    // appropriate
    copy(cinBuffer, attrInfoIVal);
  }
}

Uint32 Dbtup::copyAttrinfo(Uint32 storedProcId,
                           bool interpretedFlag)
{
  /* Get stored procedure */
  StoredProcPtr storedPtr;
  storedPtr.i = storedProcId;
  ndbrequire(c_storedProcPool.getValidPtr(storedPtr));
  ndbrequire(((storedPtr.p->storedCode == ZSCAN_PROCEDURE) ||
        (storedPtr.p->storedCode == ZCOPY_PROCEDURE)));

  const Uint32 attrinfoIVal = storedPtr.p->storedProcIVal;
  SectionReader reader(attrinfoIVal, getSectionSegmentPool());
  const Uint32 readerLen = reader.getSize();

  if (interpretedFlag)
  {
    jam();

    // Read sectionPtr's
    reader.getWords(&cinBuffer[0], 5);

    // Read interpreted sections 0..3, up to the parameter section
    const Uint32 readLen = cinBuffer[0] + cinBuffer[1] +
      cinBuffer[2] + cinBuffer[3];
    Uint32 *pos = &cinBuffer[5];
    reader.getWords(pos, readLen);
    pos += readLen;

    Uint32 paramOffs = 0;
    Uint32 paramLen = 0;
    if (cinBuffer[4] == 0)
    {
      // No parameters supplied in this attrInfo
    }
    else if (storedPtr.p->storedParamNo == 0)
    {
      // A single parameter, or the first of many, copy it out
      paramLen = cinBuffer[4];
      ndbrequire(reader.getWords(pos, paramLen));
      pos += paramLen;
      ndbassert(intmax_t{readerLen} == (pos - cinBuffer));
    }
    else
    {
      // A set of parameters, skip up to the one specified by 'ParamNo'
      for (uint i=0; i < storedPtr.p->storedParamNo; i++)
      {
        reader.getWord(pos);
        paramLen = *pos;
        reader.step(paramLen-1);
        paramOffs += paramLen;
      }
      // Copy out the parameter specified
      reader.getWord(pos);
      paramLen = *pos;
      reader.getWords(pos+1, paramLen-1);
      pos += paramLen;
    }
    cinBuffer[4] = paramLen;
  }
  else
  {
    jam();
    ndbassert(storedPtr.p->storedParamNo == 0);

    /* Copy attrInfo data into linear buffer */
    reader.getWords(&cinBuffer[0], readerLen);
  }

  // By convention we return total length of storedProc, not just what we copied.
  return readerLen;
}

void Dbtup::nextAttrInfoParam(Uint32 storedProcId)
{
  jam();

  /* Get stored procedure */
  StoredProcPtr storedPtr;
  storedPtr.i = storedProcId;
  ndbrequire(c_storedProcPool.getValidPtr(storedPtr));
  ndbrequire(((storedPtr.p->storedCode == ZSCAN_PROCEDURE) ||
        (storedPtr.p->storedCode == ZCOPY_PROCEDURE)));

  storedPtr.p->storedParamNo++;
}

void
Dbtup::setInvalidChecksum(Tuple_header *tuple_ptr,
                          const Tablerec * regTabPtr)
{
  if (regTabPtr->m_bits & Tablerec::TR_Checksum)
  {
    jam();
    /**
     * Set a magic checksum when tuple isn't supposed to be read.
     */
    tuple_ptr->m_checksum = 0x87654321;
  }
}

void
Dbtup::updateChecksum(Tuple_header *tuple_ptr,
                      const Tablerec *regTabPtr,
                      Uint32 old_header,
                      Uint32 new_header)
{
  /**
   * This function is used when only updating the header bits in row.
   * We start by XOR:ing the old header, this negates the impact of the
   * old header since old_header ^ old_header = 0. Next we XOR with new
   * header to get the new checksum and finally we store the new checksum.
   */
  if (regTabPtr->m_bits & Tablerec::TR_Checksum)
  {
    Uint32 checksum = tuple_ptr->m_checksum;
    jam();
    checksum ^= old_header;
    checksum ^= new_header;
    tuple_ptr->m_checksum = checksum;
  }
}

void
Dbtup::setChecksum(Tuple_header* tuple_ptr,
                   const Tablerec* regTabPtr)
{
  if (regTabPtr->m_bits & Tablerec::TR_Checksum)
  {
    jamDebug();
    tuple_ptr->m_checksum= 0;
    tuple_ptr->m_checksum= calculateChecksum(tuple_ptr, regTabPtr);
  }
}

Uint32
Dbtup::calculateChecksum(Tuple_header* tuple_ptr,
                         const Tablerec* regTabPtr)
{
  Uint32 checksum;
  Uint32 rec_size, *tuple_header;
  rec_size= regTabPtr->m_offsets[MM].m_fix_header_size;
  tuple_header= &tuple_ptr->m_header_bits;
  // includes tupVersion
  //printf("%p - ", tuple_ptr);

  /**
   * We include every except the first word of the Tuple header
   * which is only used on copy tuples. We do however include
   * the header bits.
   */
  checksum = computeXorChecksum(
      tuple_header, (rec_size-Tuple_header::HeaderSize) + 1);

  //printf("-> %.8x\n", checksum);

#if 0
  if (var_sized) {
    /*
       if (! req_struct->fix_var_together) {
       jam();
       checksum ^= tuple_header[rec_size];
       }
     */
    jam();
    var_data_part= req_struct->var_data_start;
    vsize_words= calculate_total_var_size(req_struct->var_len_array,
        regTabPtr->no_var_attr);
    ndbassert(req_struct->var_data_end >= &var_data_part[vsize_words]);
    checksum = computeXorChecksum(var_data_part,vsize_words,checksum);
  }
#endif
  return checksum;
}

int
Dbtup::corruptedTupleDetected(KeyReqStruct *req_struct, Tablerec *regTabPtr)
{
  Uint32 checksum = calculateChecksum(req_struct->m_tuple_ptr, regTabPtr);
  Uint32 header_bits = req_struct->m_tuple_ptr->m_header_bits;
  Uint32 tableId = req_struct->fragPtrP->fragTableId;
  Uint32 fragId = req_struct->fragPtrP->fragmentId;
  Uint32 page_id = req_struct->frag_page_id;
  Uint32 page_idx = prepare_page_idx;

  g_eventLogger->info(
      "Tuple corruption detected, checksum: 0x%x, header_bits: 0x%x"
      ", checksum word: 0x%x"
      ", tab(%u,%u), page(%u,%u)",
      checksum, header_bits, req_struct->m_tuple_ptr->m_checksum, tableId,
      fragId, page_id, page_idx);
  if (c_crashOnCorruptedTuple && !ERROR_INSERTED(4036))
  {
    g_eventLogger->info(" Exiting.");
    ndbabort();
  }
  (void)ERROR_INSERTED_CLEAR(4036);
  terrorCode= ZTUPLE_CORRUPTED_ERROR;
  tupkeyErrorLab(req_struct);
  return -1;
}

/* ----------------------------------------------------------------- */
/* -----------       INSERT_ACTIVE_OP_LIST            -------------- */
/* ----------------------------------------------------------------- */
bool 
Dbtup::prepareActiveOpList(OperationrecPtr regOperPtr,
                           KeyReqStruct* req_struct)
{
  /**
   * We are executing in the LDM thread since this is a write operation.
   * Thus we are protected from concurrent write activity from other
   * threads. We are however not protected against READ activities in the
   * query thread. Readers use the linked list of operations on the
   * row to find out which version of the row to use.
   *
   * We cannot publish our new row version until it is fully written,
   * thus it is ok to become the new leader of the write operations since
   * we are protected from other write row activity, but it is not ok to
   * change the linked list of operations on the row until we have completed
   * the write of the row.
   *
   * Therefore we divide insertActiveOpList into a prepareActiveOpList and
   * later call insertActiveOpList when the write is completed and we are
   * ready to insert ourselves into the linked list of operations on the
   * record.
   *
   * For initial inserts we place ourselves into the linked list immediately
   * since REFRESH operations are always performed with exclusive
   * access to the fragment and thus no interaction with query threads is
   * possible.
   */
  jam();
  OperationrecPtr prevOpPtr;
  ndbrequire(!regOperPtr.p->op_struct.bit_field.in_active_list);
  req_struct->prevOpPtr.i= 
    prevOpPtr.i= req_struct->m_tuple_ptr->m_operation_ptr_i;
  regOperPtr.p->prevActiveOp= prevOpPtr.i;
  regOperPtr.p->m_undo_buffer_space= 0;
  ndbassert(!m_is_in_query_thread);
  if (likely(prevOpPtr.i == RNIL))
  {
    return true;
  }
  else
  {
    jam();
    jamLineDebug(Uint16(prevOpPtr.i));
    ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(prevOpPtr));
    req_struct->prevOpPtr.p = prevOpPtr.p;

    regOperPtr.p->op_struct.bit_field.m_wait_log_buffer= 
      prevOpPtr.p->op_struct.bit_field.m_wait_log_buffer;
    regOperPtr.p->op_struct.bit_field.m_load_diskpage_on_commit= 
      prevOpPtr.p->op_struct.bit_field.m_load_diskpage_on_commit;
    regOperPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit= 
      prevOpPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit;
    regOperPtr.p->op_struct.bit_field.m_gci_written=
      prevOpPtr.p->op_struct.bit_field.m_gci_written;
    regOperPtr.p->op_struct.bit_field.m_tuple_existed_at_start=
      prevOpPtr.p->op_struct.bit_field.m_tuple_existed_at_start;
    regOperPtr.p->m_undo_buffer_space= prevOpPtr.p->m_undo_buffer_space;
    regOperPtr.p->m_uncommitted_used_space =
      prevOpPtr.p->m_uncommitted_used_space;
    // start with prev mask (matters only for UPD o UPD)

    regOperPtr.p->m_any_value = prevOpPtr.p->m_any_value;

    prevOpPtr.p->op_struct.bit_field.m_wait_log_buffer= 0;
    prevOpPtr.p->op_struct.bit_field.m_load_diskpage_on_commit= 0;
    prevOpPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit= 0;

    if (prevOpPtr.p->tuple_state == TUPLE_PREPARED)
    {
      Uint32 op= regOperPtr.p->op_type;
      Uint32 prevOp= prevOpPtr.p->op_type;
      if (prevOp == ZDELETE)
      {
        if(op == ZINSERT)
        {
          // mark both
          prevOpPtr.p->op_struct.bit_field.delete_insert_flag= true;
          regOperPtr.p->op_struct.bit_field.delete_insert_flag= true;
          return true;
        }
        else if (op == ZREFRESH)
        {
          /* ZREFRESH after Delete - ok */
          return true;
        }
        else
        {
          terrorCode= ZTUPLE_DELETED_ERROR;
          return false;
        }
      } 
      else if(op == ZINSERT && prevOp != ZDELETE)
      {
        terrorCode= ZINSERT_ERROR;
        return false;
      }
      else if (prevOp == ZREFRESH)
      {
        /* No operation after a ZREFRESH */
        terrorCode= ZOP_AFTER_REFRESH_ERROR;
        return false;
      }
      return true;
    }
    else
    {
      terrorCode= ZMUST_BE_ABORTED_ERROR;
      return false;
    }
  }
}

void
Dbtup::insertActiveOpList(OperationrecPtr regOperPtr,
                          KeyReqStruct* req_struct,
                          Tuple_header *tuple_ptr)
{
  /**
   * We have already prepared inserting ourselves into the list by
   * setting prevActiveOp to point to the previous leader.
   * We have not yet put ourselves last in the list, this is done
   * by updating the row operation pointer and by updating nextActiveOp
   * to point to us. We do this after performing the changes to ensure
   * that inserting us in the list happens after performing the changes
   * related to the operation.
   */
  jamDebug();
  jamDataDebug(regOperPtr.i);
  regOperPtr.p->op_struct.bit_field.in_active_list = true;
  tuple_ptr->m_operation_ptr_i = regOperPtr.i;
  if (unlikely(req_struct->prevOpPtr.i != RNIL))
  {
    jam();
    req_struct->prevOpPtr.p->nextActiveOp = regOperPtr.i;
  }
}

bool
Dbtup::setup_read(KeyReqStruct *req_struct,
                  Operationrec* regOperPtr,
                  Tablerec* regTabPtr,
                  bool disk)
{
  OperationrecPtr currOpPtr;
  currOpPtr.i= req_struct->m_tuple_ptr->m_operation_ptr_i;
  const Uint32 bits = req_struct->m_tuple_ptr->m_header_bits;

  if (unlikely(req_struct->m_reorg != ScanFragReq::REORG_ALL))
  {
    const Uint32 moved = bits & Tuple_header::REORG_MOVE;
    if (! ((req_struct->m_reorg == ScanFragReq::REORG_NOT_MOVED &&
            moved == 0) ||
          (req_struct->m_reorg == ScanFragReq::REORG_MOVED && moved != 0)))
    {
      /**
       * We're either scanning to only find moved rows (used when scanning
       * for rows to delete in reorg delete phase or we're scanning for
       * only non-moved rows and this happens also in reorg delete phase,
       * but it is done for normal scans in this phase.
       */
      jamDebug();
      terrorCode= ZTUPLE_DELETED_ERROR;
      return false;
    }
  }
  if (likely(currOpPtr.i == RNIL))
  {
    jamDebug();
    if (regTabPtr->need_expand(disk))
    {
      jamDebug();
      prepare_read(req_struct, regTabPtr, disk);
    }
    return true;
  }

  do {
    Uint32 savepointId= regOperPtr->savepointId;
    bool dirty= req_struct->dirty_op;
    Dblqh *ldm_lqh = nullptr;
    Dbtup *ldm_tup = this;

    /**
     * currOpPtr.i is an operation record in the LDM thread owning
     * the fragment. We could however be a query thread, we have
     * setup m_ldm_instance_used to always point to the owning
     * LDM threads block instance for DBLQH, DBTUP and DBACC.
     */
    currOpPtr.p = m_ldm_instance_used->getOperationPtrP(currOpPtr.i);
    ldm_lqh = c_lqh->m_ldm_instance_used;
    ldm_tup = m_ldm_instance_used;

    const bool sameTrans= ldm_lqh->is_same_trans(currOpPtr.p->userpointer,
        req_struct->trans_id1,
        req_struct->trans_id2);
    /**
     * Read committed in same trans reads latest copy
     */
    if(dirty && !sameTrans)
    {
      jamDebug();
      savepointId= 0;
    }
    else if(sameTrans)
    {
      // Use savepoint even in read committed mode
      jamDebug();
      dirty= false;
    }

    /* found == true indicates that savepoint is some state
     * within tuple's current transaction's uncommitted operations
     */
    const bool found = ldm_tup->find_savepoint(currOpPtr,
        savepointId,
        jamBuffer());

    const Uint32 currOp= currOpPtr.p->op_type;

    /* is_insert==true if tuple did not exist before its current
     * transaction
     */
    const bool is_insert = (bits & Tuple_header::ALLOC);

    /* If savepoint is in transaction, and post-delete-op
     *   OR
     * Tuple didn't exist before
     *      AND
     *   Read is dirty
     *           OR
     *   Savepoint is before-transaction
     *
     * Tuple does not exist in read's view
     */
    if((found && currOp == ZDELETE) || 
        ((dirty || !found) && is_insert))
    {
      /* Tuple not visible to this read operation */
      jamDebug();
      terrorCode= ZTUPLE_DELETED_ERROR;
      break;
    }

    if(dirty || !found)
    {
      /* Read existing committed tuple */
      jamDebug();
    }
    else
    {
      jamDebug();
      req_struct->m_tuple_ptr=
        get_copy_tuple(&currOpPtr.p->m_copy_tuple_location);
    }

    if (regTabPtr->need_expand(disk))
    {
      jamDebug();
      prepare_read(req_struct, regTabPtr, disk);
    }
    return true;
  } while(0);

  return false;
}

int
Dbtup::load_diskpage(Signal* signal,
                     Uint32 opRec,
                     Uint32 lkey1,
                     Uint32 lkey2,
                     Uint32 flags)
{
  Ptr<Operationrec> operPtr;

  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));

  Operationrec *  regOperPtr= operPtr.p;
  Fragrecord * regFragPtr= prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;

  if (Local_key::isInvalid(lkey1, lkey2))
  {
    jam();
    regOperPtr->op_struct.bit_field.m_wait_log_buffer= 1;
    regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 1;
    if (unlikely((flags & 7) == ZREFRESH))
    {
      jam();
      /* Refresh of previously nonexistent DD tuple.
       * No diskpage to load at commit time
       */
      regOperPtr->op_struct.bit_field.m_wait_log_buffer= 0;
      regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 0;
    }

    /* In either case return 1 for 'proceed' */
    return 1;
  }

  jam();
  ndbassert(Uint16(lkey2) == lkey2);
  Uint16 page_idx= Uint16(lkey2);
  Uint32 frag_page_id= lkey1;
  regOperPtr->m_tuple_location.m_page_no= getRealpid(regFragPtr,
      frag_page_id);
  regOperPtr->m_tuple_location.m_page_idx= page_idx;

  PagePtr page_ptr;
  Uint32* tmp= get_ptr(&page_ptr, &regOperPtr->m_tuple_location, regTabPtr);
  Tuple_header* ptr= (Tuple_header*)tmp;

  if (((flags & 7) == ZREAD) &&
      ptr->m_header_bits & Tuple_header::DELETE_WAIT)
  {
    jam();
    /**
     * Tuple is already deleted and must not be read at this point in
     * time since when we come back from real-time break the row
     * will already be removed and invalidated.
     */
    return -(TUP_NO_TUPLE_FOUND);
  }
  int res= 1;
  if (ptr->m_header_bits & Tuple_header::DISK_PART ||
      ptr->m_header_bits & Tuple_header::DISK_VAR_PART)
  {
    jam();
    /**
     * We retrieve the original disk row page when the transaction
     * started with an existing disk row (DISK_PART flag is set).
     * When we arrive here and DISK_PART isn't set, but DISK_VAR_PART
     * is set, this means that this is an operation that started with
     * an initial insert of a row. Any updates or re-inserts of this
     * row in the same transaction requires the page where the row is
     * allocated to be read before the operation is started. This is
     * necessary on variable sized disk rows since we need to check
     * if the row still fits on the page after performing this operation.
     */
    Page_cache_client::Request req;
    memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
    req.m_table_id = regFragPtr->fragTableId;
    req.m_fragment_id = regFragPtr->fragmentId;
    req.m_callback.m_callbackData= opRec;
    req.m_callback.m_callbackFunction= 
      safe_cast(&Dbtup::disk_page_load_callback);

#ifdef ERROR_INSERT
    if (ERROR_INSERTED(4022))
    {
      flags |= Page_cache_client::DELAY_REQ;
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      req.m_delay_until_time = NdbTick_AddMilliseconds(now,(Uint64)3000);
    }
    if (ERROR_INSERTED(4035) && (rand() % 13) == 0)
    {
      // Disk access have to randomly wait max 16ms for a diskpage
      Uint64 delay = (Uint64)(rand() % 16) + 1;
      flags |= Page_cache_client::DELAY_REQ;
      const NDB_TICKS now = NdbTick_getCurrentTicks();
      req.m_delay_until_time = NdbTick_AddMilliseconds(now,delay);
    }
#endif

    if (regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit)
    {
      /**
       * We will request 2 pages and need to ensure that the first page
       * isn't paged out while we are paging in the second page.
       */
      flags |= Page_cache_client::REF_REQ;
    }
    Page_cache_client pgman(this, c_pgman);
    res= pgman.get_page(signal, req, flags);
  }

  switch(flags & 7)
  {
    case ZREAD:
    case ZREAD_EX:
      break;
    case ZDELETE:
    case ZUPDATE:
    case ZINSERT:
    case ZWRITE:
    case ZREFRESH:
      jam();
      regOperPtr->op_struct.bit_field.m_wait_log_buffer= 1;
      regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 1;
  }
  if (res > 0)
  {
    jam();
    regOperPtr->m_disk_callback_page = res;
    regOperPtr->m_disk_extra_callback_page = RNIL;
    if (regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit)
    {
      jam();
      res = load_extra_diskpage(signal, opRec, flags);
    }
  }
  return res;
}

int
Dbtup::load_extra_diskpage(Signal *signal, Uint32 opRec, Uint32 flags)
{
  Fragrecord * regFragPtr = prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  PagePtr page_ptr;
  ndbassert(!operPtr.p->m_copy_tuple_location.isNull());
  Tuple_header *ptr = get_copy_tuple(&operPtr.p->m_copy_tuple_location);
  jamEntry();
  /**
   * We will never need an extra disk page if the first operation was an
   * INSERT operation. This means that DISK_PART must be set on the row.
   */
  ndbrequire(ptr->m_header_bits & Tuple_header::DISK_PART);
  Page_cache_client::Request req;
  memcpy(&req.m_page,
      ptr->get_disk_ref_ptr(regTabPtr),
      sizeof(Local_key));
  req.m_table_id = regFragPtr->fragTableId;
  req.m_fragment_id = regFragPtr->fragmentId;
  req.m_callback.m_callbackData= opRec;
  req.m_callback.m_callbackFunction= 
    safe_cast(&Dbtup::disk_page_load_extra_callback);

  Page_cache_client pgman(this, c_pgman);
  int res = pgman.get_page(signal, req, flags);
  ndbrequire(res < 0);
  if (res > 0)
  {
    jam();
    operPtr.p->m_disk_extra_callback_page = Uint32(res);
    deref_disk_page(signal,
        operPtr,
        regFragPtr,
        regTabPtr);
  }
  return res;
}

void
Dbtup::deref_disk_page(Signal *signal,
                       OperationrecPtr operPtr,
                       Fragrecord *regFragPtr,
                       Tablerec *regTabPtr)
{
  PagePtr page_ptr;
  Tuple_header* ptr;
  jamDebug();
  Uint32* tmp= get_ptr(&page_ptr, &operPtr.p->m_tuple_location, regTabPtr);
  ptr = (Tuple_header*)tmp;
  Page_cache_client::Request req;
  memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
  req.m_table_id = regFragPtr->fragTableId;
  req.m_fragment_id = regFragPtr->fragmentId;
  req.m_callback.m_callbackData = operPtr.i;
  req.m_callback.m_callbackFunction= 
    safe_cast(&Dbtup::deref_disk_page_callback);
  Page_cache_client pgman(this, c_pgman);
  Uint32 flags = Page_cache_client::DEREF_REQ;
  int res = pgman.get_page(signal, req, flags);
  ndbrequire(res > 0);
}

void
Dbtup::deref_disk_page_callback(Signal *signal, Uint32 opRec, Uint32 page_id)
{
  (void)signal;
  (void)opRec;
  (void)page_id;
  ndbabort();
}

void
Dbtup::disk_page_load_callback(Signal* signal, Uint32 opRec, Uint32 page_id)
{
  Ptr<Operationrec> operPtr;
  jam();
  operPtr.i = opRec;
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  if (operPtr.p->op_struct.bit_field.m_load_extra_diskpage_on_commit)
  {
    jam();
    c_lqh->setup_key_pointers(operPtr.p->userpointer);
    Uint32 flags = c_lqh->get_pgman_flags();
    Uint32 extra_page_id = (Uint32)load_extra_diskpage(signal, opRec, flags);
    if (extra_page_id == 0)
    {
      /* Save the disk callback page during real-time break. */
      operPtr.p->m_disk_callback_page = page_id;
      return;
    }
  }
  else
  {
    /**
     * The m_disk_callback_page will be overwritten, thus we pass it
     * to DBLQH so that DBLQH can set it up.
     */
    ;
    jam();
    c_lqh->setup_key_pointers(operPtr.p->userpointer);
    operPtr.p->m_disk_callback_page = page_id;
  }
  c_lqh->acckeyconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      page_id);
}

void
Dbtup::disk_page_load_extra_callback(Signal* signal,
                                     Uint32 opRec,
                                     Uint32 extra_page_id)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  jam();
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  operPtr.p->m_disk_extra_callback_page = extra_page_id;
  Uint32 page_id = operPtr.p->m_disk_callback_page;
  c_lqh->setup_key_pointers(operPtr.p->userpointer);
  operPtr.p->m_disk_callback_page = page_id;
  Fragrecord * regFragPtr = prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;
  deref_disk_page(signal,
      operPtr,
      regFragPtr,
      regTabPtr);
  c_lqh->acckeyconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      operPtr.p->m_disk_callback_page);
}

int
Dbtup::load_diskpage_scan(Signal* signal,
                          Uint32 opRec,
                          Uint32 lkey1,
                          Uint32 lkey2,
                          Uint32 tux_flag,
                          Uint32 disk_flag)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));

  Operationrec *  regOperPtr= operPtr.p;
  Fragrecord * regFragPtr= prepare_fragptr.p;
  Tablerec* regTabPtr = prepare_tabptr.p;

  jam();
  Uint32 page_idx= lkey2;
  if (likely(tux_flag))
  {
    jamDebug();
    regOperPtr->m_tuple_location.m_page_no = lkey1;
  }
  else
  {
    jamDebug();
    Uint32 frag_page_id= lkey1;
    regOperPtr->m_tuple_location.m_page_no= getRealpid(regFragPtr,
        frag_page_id);
  }
  regOperPtr->m_tuple_location.m_page_idx= page_idx;
  regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit= 0;

  PagePtr page_ptr;
  Uint32* tmp= get_ptr(&page_ptr, &regOperPtr->m_tuple_location, regTabPtr);
  Tuple_header* ptr= (Tuple_header*)tmp;

  if (ptr->m_header_bits & Tuple_header::DELETE_WAIT)
  {
    jam();
    /**
     * Tuple is already deleted and must not be read at this point in
     * time since when we come back from real-time break the row
     * will already be removed and invalidated.
     */
    return -(TUP_NO_TUPLE_FOUND);
  }

  int res= 1;
  if (ptr->m_header_bits & Tuple_header::DISK_PART)
  {
    jam();
    Page_cache_client::Request req;
    memcpy(&req.m_page, ptr->get_disk_ref_ptr(regTabPtr), sizeof(Local_key));
    req.m_table_id = regFragPtr->fragTableId;
    req.m_fragment_id = regFragPtr->fragmentId;
    req.m_callback.m_callbackData= opRec;
    req.m_callback.m_callbackFunction= 
      safe_cast(&Dbtup::disk_page_load_scan_callback);

    Page_cache_client pgman(this, c_pgman);
    res= pgman.get_page(signal, req, disk_flag);
    if (res > 0)
    {
      regOperPtr->m_disk_callback_page = res;
    }
  }
  else
  {
    jam();
    /**
     * We need to set m_disk_callback_page to something different
     * than RNIL to indicate that we should be ready to read the
     * disk columns. At the same time there is no disk page, so
     * we set it to something that should crash if attempted to
     * be used as a page id.
     */
    regOperPtr->m_disk_callback_page = Uint32(~0);
  }
  regOperPtr->m_disk_extra_callback_page = RNIL;
  return res;
}

void
Dbtup::disk_page_load_scan_callback(Signal* signal, 
                                    Uint32 opRec,
                                    Uint32 page_id)
{
  Ptr<Operationrec> operPtr;
  operPtr.i = opRec;
  jam();
  jamData(opRec);
  ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
  c_lqh->next_scanconf_load_diskpage_callback(signal, 
      operPtr.p->userpointer,
      page_id);
}

/**
  This method is used to prepare for faster execution of TUPKEYREQ.
  It prepares the pointers to the fragment record, the table record,
  the page for the record and the tuple pointer to the record. In
  addition it also prefetches the cache lines of the fixed size part
  of the tuple.

  The calculations performed here have to be done when we arrive in
  execTUPKEYREQ, we perform them here to enable prefetching the
  cache lines of the fixed part of the tuple storage. In order to not
  do the same work twice we store the calculated information in
  block variables. Given that we can arrive in execTUPKEYREQ from
  multiple directions, we have added debug-code that verifies that we
  have passed through prepareTUPKEYREQ always before we reach
  execTUPKEYREQ.

  The access of the fixed size part of the tuple is an almost certain
  CPU cache miss and so performing this as early as possible will
  decrease the time for cache misses later in the process. Tests using
  Sysbench indicates that this prefetch gains about 5% in performance.

  See DblqhMain.cpp for more documentation of prepare_* methods.
 */

void Dbtup::prepare_tab_pointers_acc(Uint32 table_id, Uint32 frag_id)
{
  TablerecPtr tablePtr;
  tablePtr.i = table_id;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);
  FragrecordPtr fragPtr;
  getFragmentrec(fragPtr, frag_id, tablePtr.i);
  ndbrequire(fragPtr.i != RNIL64);
  prepare_fragptr = fragPtr;
  prepare_tabptr = tablePtr;
}

void Dbtup::prepareTUPKEYREQ(Uint32 page_id,
    Uint32 page_idx,
    Uint64 fragPtrI)
{
  FragrecordPtr fragptr;
  TablerecPtr tabptr;

  fragptr.i = fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragptr));
  const Uint32 RnoOfTablerec= cnoOfTablerec;
  Tablerec * Rtablerec = tablerec;

  jamEntryDebug();
  tabptr.i = fragptr.p->fragTableId;
  ptrCheckGuard(tabptr, RnoOfTablerec, Rtablerec);
  prepare_tabptr = tabptr;
  prepare_fragptr = fragptr;
  prepare_scanTUPKEYREQ(page_id, page_idx);
}

void Dbtup::prepare_scanTUPKEYREQ(Uint32 page_id, Uint32 page_idx)
{
  Local_key key;
  PagePtr pagePtr;
#ifdef VM_TRACE
  prepare_orig_local_key.m_page_no = page_id;
  prepare_orig_local_key.m_page_idx = page_idx;
#endif
  bool is_page_key = (!(Local_key::isInvalid(page_id, page_idx) ||
        isCopyTuple(page_id, page_idx)));

  if (is_page_key)
  {
    Uint32 fixed_part_size_in_words =
      prepare_tabptr.p->m_offsets[MM].m_fix_header_size;
    acquire_frag_page_map_mutex_read(prepare_fragptr.p, jamBuffer());
    page_id = getRealpid(prepare_fragptr.p, page_id);
    release_frag_page_map_mutex_read(prepare_fragptr.p, jamBuffer());
    key.m_page_no = page_id;
    key.m_page_idx = page_idx;
    Uint32 *tuple_ptr = get_ptr(&pagePtr,
        &key,
        prepare_tabptr.p);
    jamDebug();
    prepare_pageptr = pagePtr;
    prepare_page_idx = page_idx;
    prepare_tuple_ptr = tuple_ptr;
    prepare_page_no = page_id;
    for (Uint32 i = 0; i < fixed_part_size_in_words; i+= 16)
    {
      NDB_PREFETCH_WRITE(tuple_ptr + i);
    }
  }
}

void Dbtup::prepare_scan_tux_TUPKEYREQ(Uint32 page_id, Uint32 page_idx)
{
  Local_key key;
  PagePtr pagePtr;
#ifdef VM_TRACE
  prepare_orig_local_key.m_page_no = page_id;
  prepare_orig_local_key.m_page_idx = page_idx;
#endif
  bool is_page_key = (!(Local_key::isInvalid(page_id, page_idx) ||
        isCopyTuple(page_id, page_idx)));

  ndbrequire(is_page_key);
  {
    Uint32 fixed_part_size_in_words =
      prepare_tabptr.p->m_offsets[MM].m_fix_header_size;
    key.m_page_no = page_id;
    key.m_page_idx = page_idx;
    Uint32 *tuple_ptr = get_ptr(&pagePtr,
        &key,
        prepare_tabptr.p);
    jamDebug();
    prepare_pageptr = pagePtr;
    prepare_tuple_ptr = tuple_ptr;
    prepare_page_no = page_id;
    for (Uint32 i = 0; i < fixed_part_size_in_words; i+= 16)
    {
      NDB_PREFETCH_WRITE(tuple_ptr + i);
    }
  }
}

bool Dbtup::execTUPKEYREQ(Signal* signal,
                          void *_lqhOpPtrP,
                          void *_lqhScanPtrP)
{
  Dblqh::TcConnectionrec *lqhOpPtrP = (Dblqh::TcConnectionrec*)_lqhOpPtrP;
  Dblqh::ScanRecord *lqhScanPtrP = (Dblqh::ScanRecord*)_lqhScanPtrP;

  TupKeyReq * tupKeyReq= (TupKeyReq *)signal->getDataPtr();
  Ptr<Operationrec> operPtr = prepare_oper_ptr;
  KeyReqStruct req_struct(this);

  jamEntryDebug();
  jamLineDebug(Uint16(prepare_oper_ptr.i));
  req_struct.m_lqh = c_lqh;

#ifdef VM_TRACE
  {
    bool error_found = false;
    Local_key key;
    key.m_page_no = tupKeyReq->keyRef1;
    key.m_page_idx = tupKeyReq->keyRef2;
    if (key.m_page_no != prepare_orig_local_key.m_page_no)
    {
      ndbout << "page_no = " << prepare_orig_local_key.m_page_no;
      ndbout << " keyRef1 = " << key.m_page_no << endl;
      error_found = true;
    }
    if (key.m_page_idx != prepare_orig_local_key.m_page_idx)
    {
      ndbout << "page_idx = " << prepare_orig_local_key.m_page_idx;
      ndbout << " keyRef2 = " << key.m_page_idx << endl;
      error_found = true;
    }
    if (error_found)
    {
      ndbout << flush;
    }
    ndbassert(prepare_orig_local_key.m_page_no == key.m_page_no);
    ndbassert(prepare_orig_local_key.m_page_idx == key.m_page_idx);
    FragrecordPtr fragPtr = prepare_fragptr;
    ndbrequire(c_fragment_pool.getPtr(fragPtr));
    ndbassert(prepare_fragptr.p == fragPtr.p);
  }
#endif

  /**
   * DESIGN PATTERN DESCRIPTION
   * --------------------------
   * The variable operPtr.p is located on the block object, it is located
   * there to ensure that we can easily access it in many methods such
   * that we don't have to transport it through method calls. There are
   * a number of references to structs that we store in this manner.
   * Oftentimes they refer to the operation object, the table object,
   * the fragment object and sometimes also a transaction object.
   *
   * Given that we both need access to the .i-value and the .p-value
   * of all of those objects we store them on the block object to
   * avoid the need of transporting them from function to function.
   * This is an optimisation and obviously requires that one keeps
   * track of which variables are alive and which are not.
   * The function clear_global_variables used in debug mode ensures
   * that all pointer variables are cleared before an asynchronous
   * signal is executed.
   *
   * When we need to access data through the .p-value many times
   * (more than one time), then it often pays off to declare a
   * stack variable such as below regOperPtr. This helps the compiler
   * to avoid having to constantly reload the .p-value from the
   * block object after each store operation through a pointer.
   *
   * One has to take care though when doing this to ensure that
   * one doesn't create a stack variable that creates too much
   * pressure on the register allocation in the method. This is
   * particularly important in large methods.
   *
   * The pattern is to define the variable as:
   * Operationrec * const regOperPtr = operPtr.p;
   * This helps the compiler to understand that we won't change the
   * pointer here.
   */
  Operationrec * const regOperPtr= operPtr.p;

  Dbtup::TransState trans_state = get_trans_state(regOperPtr);

  req_struct.signal= signal;
  req_struct.operPtrP = regOperPtr;
  regOperPtr->fragmentPtr = prepare_fragptr.i;
  regOperPtr->prevActiveOp = RNIL;
  regOperPtr->nextActiveOp = RNIL;
  req_struct.num_fired_triggers= 0;
  req_struct.no_exec_instructions = 0;
  req_struct.read_length= 0;
  req_struct.last_row= false;
  req_struct.m_is_lcp = false;

  if (unlikely(trans_state != TRANS_IDLE))
  {
    TUPKEY_abort(&req_struct, 39);
    return false;
  }

  /* ----------------------------------------------------------------- */
  // Operation is ZREAD when we arrive here so no need to worry about the
  // abort process.
  /* ----------------------------------------------------------------- */
  /* -----------    INITIATE THE OPERATION RECORD       -------------- */
  /* ----------------------------------------------------------------- */
  Uint32 disable_fk_checks = 0;
  Uint32 deferred_constraints = 0;
  Uint32 flags = lqhOpPtrP->m_flags;
  if (lqhScanPtrP != nullptr)
  {
    Uint32 attrBufLen = lqhScanPtrP->scanAiLength;
    Uint32 dirtyOp = (lqhScanPtrP->scanLockHold == ZFALSE);
    Uint32 prioAFlag = lqhScanPtrP->prioAFlag;
    Uint32 opRef = lqhScanPtrP->scanApiOpPtr;
    Uint32 applRef = lqhScanPtrP->scanApiBlockref;
    Uint32 interpreted_exec = lqhOpPtrP->opExec;

    req_struct.log_size = attrBufLen;
    req_struct.attrinfo_len = attrBufLen;
    req_struct.dirty_op = dirtyOp;
    req_struct.m_prio_a_flag = prioAFlag;
    req_struct.tc_operation_ptr = opRef;
    req_struct.rec_blockref= applRef;
    req_struct.interpreted_exec = interpreted_exec;
    req_struct.m_nr_copy_or_redo = 0;
    req_struct.m_use_rowid = 0;
#ifdef ERROR_INSERT
    /* Insert garbage into rowid, should not be used */
    req_struct.m_row_id.m_page_no = RNIL;
    req_struct.m_row_id.m_page_idx = ZNIL;
#endif
  }
  else
  {
    Uint32 attrBufLen = lqhOpPtrP->totReclenAi;
    Uint32 dirtyOp = lqhOpPtrP->dirtyOp;
    Uint32 row_id = TupKeyReq::getRowidFlag(tupKeyReq->request);
    Uint32 interpreted_exec =
      TupKeyReq::getInterpretedFlag(tupKeyReq->request);
    Uint32 opRef = lqhOpPtrP->applOprec;
    Uint32 applRef = lqhOpPtrP->applRef;

    req_struct.dirty_op = dirtyOp;
    req_struct.m_use_rowid = row_id;
    req_struct.log_size = attrBufLen;
    req_struct.attrinfo_len = attrBufLen;
    req_struct.tc_operation_ptr = opRef;
    req_struct.rec_blockref= applRef;
    req_struct.interpreted_exec = interpreted_exec;

    req_struct.m_prio_a_flag = 0;
    req_struct.m_nr_copy_or_redo =
      ((LqhKeyReq::getNrCopyFlag(lqhOpPtrP->reqinfo) |
        c_lqh->c_executing_redo_log) != 0);
    disable_fk_checks =
      ((flags & Dblqh::TcConnectionrec::OP_DISABLE_FK) != 0);
    deferred_constraints =
      ((flags & Dblqh::TcConnectionrec::OP_DEFERRED_CONSTRAINTS) != 0);
    const Uint32 row_id_page_no = tupKeyReq->m_row_id_page_no;
    const Uint32 row_id_page_idx = tupKeyReq->m_row_id_page_idx;
    req_struct.m_row_id.m_page_no = row_id_page_no;
    req_struct.m_row_id.m_page_idx = row_id_page_idx;
  }
  req_struct.m_deferred_constraints = deferred_constraints;
  req_struct.m_disable_fk_checks = disable_fk_checks;
  {
    Operationrec::OpStruct op_struct;
    op_struct.op_bit_fields = regOperPtr->op_struct.op_bit_fields;
    op_struct.bit_field.m_disable_fk_checks = disable_fk_checks;
    op_struct.bit_field.m_deferred_constraints = deferred_constraints;

    const Uint32 triggers =
      (flags & Dblqh::TcConnectionrec::OP_NO_TRIGGERS) ?
      TupKeyReq::OP_NO_TRIGGERS :
      (lqhOpPtrP->seqNoReplica == 0) ?
      TupKeyReq::OP_PRIMARY_REPLICA : TupKeyReq::OP_BACKUP_REPLICA;
    op_struct.bit_field.delete_insert_flag = false;
    op_struct.bit_field.m_gci_written = 0;
    op_struct.bit_field.m_reorg = lqhOpPtrP->m_reorg;
    op_struct.bit_field.tupVersion= ZNIL;
    op_struct.bit_field.m_triggers = triggers;

    regOperPtr->m_copy_tuple_location.setNull();
    regOperPtr->op_struct.op_bit_fields = op_struct.op_bit_fields;
  }
  {
    Uint32 reorg = lqhOpPtrP->m_reorg;
    Uint32 op = lqhOpPtrP->operation;

    req_struct.m_reorg = reorg;
    regOperPtr->op_type = op;
  }
  {
    /**
     * DESIGN PATTERN DESCRIPTION
     * --------------------------
     * This code segment is using a common design pattern in the
     * signal reception and signal sending code of performance
     * critical functions such as execTUPKEYREQ.
     * The idea is that at signal reception we need to transfer
     * data from the signal object to state variables relating to
     * the operation we are about to execute.
     * The normal manner to do this would be to write:
     * regOperPtr->savePointId = tupKeyReq->savePointId;
     * 
     * This normal manner would however not work so well due to
     * that the compiler has to issue assembler code that does
     * a load operation immediately followed by a store operation.
     * Many modern CPUs can hide parts of this deficiency in the
     * code, but only to a certain extent.
     *
     * What we want to do here is instead to perform a series of
     * six loads followed by six stores. The delay after a load
     * is ready for a store operation is oftentimes 3 cycles. Many
     * CPUs can handle two loads per cycle. So by using 6 loads
     * we ensure that we execute at full speed as long as the data
     * is available in the first level CPU cache.
     *
     * The reason we don't want to use more than 6 loads before
     * we start storing is that CPUs have a limited amount of
     * CPU registers. The x86 have 16 CPU registers available.
     * Here is a short description of commonly used registers:
     * RIP: Instruction pointer, not available
     * RSP: Top of Stack pointer, not available for L/S
     * RBP: Current Stack frame pointer, not available for L/S
     * RDI: Usually this-pointer, reference to Dbtup object here
     * 
     * In this particular example we also need to have a register
     * for storing:
     * tupKeyReq, req_struct, regOperPtr.
     *
     * The compiler also needs a few more registers to track some
     * of the other live variables such that not all of the live
     * variables have to be spilled to the stack.
     *
     * Thus the design pattern uses between 4 to 6 variables loaded
     * before storing them. Another commonly used manner is to locate
     * all initialisations to constants in one or more of those
     * initialisation code blocks as well.
     *
     * The naming pattern is to define the temporary variable as
     * const Uint32 name_of_variable_to_assign = x->name;
     * y->name_of_variable_to_assign = name_of_variable_to_assign.
     * 
     * In the case where the receiver of the data is a signal object
     * we use the pattern:
     * const Uint32 sig0 = x->name;
     * signal->theData[0] = sig0;
     *
     * Finally if possible we should place this initialisation in a
     * separate code block by surrounding it with brackets, this is
     * to assist the compiler to understand that the variables used
     * are not needed after storing its value. Most compilers will
     * handle this well anyways, but it helps the compiler avoid
     * doing mistakes and it also clarifies for the reader of the
     * source code. As can be seen in code below this rule is
     * however not followed if it will remove other possibilities.
     */
    const Uint32 savePointId = lqhOpPtrP->savePointId;
    const Uint32 tcOpIndex = lqhOpPtrP->tcOprec;
    const Uint32 coordinatorTC = lqhOpPtrP->tcBlockref;

    regOperPtr->savepointId = savePointId;
    req_struct.TC_index = tcOpIndex;
    req_struct.TC_ref = coordinatorTC;
  }

  const Uint32 disk_page = regOperPtr->m_disk_callback_page;
  const Uint32 keyRef1 = tupKeyReq->keyRef1;
  const Uint32 keyRef2 = tupKeyReq->keyRef2;

  req_struct.m_disk_page_ptr.i = disk_page;
  /**
   * The pageid here is a page id of a row id except when we are
   * reading from an ordered index scan, in this case it is a
   * physical page id. We will only use this variable for LCP
   * scan reads and for inserts and refreshs. So it is not used
   * for TUX scans.
   */
  Uint32 pageid = regOperPtr->fragPageId = req_struct.frag_page_id = keyRef1;
  Uint32 pageidx = regOperPtr->m_tuple_location.m_page_idx = keyRef2;

  const Uint32 transId1 = lqhOpPtrP->transid[0];
  const Uint32 transId2 = lqhOpPtrP->transid[1];
  Tablerec * const regTabPtr = prepare_tabptr.p;

  /* Get AttrInfo section if this is a long TUPKEYREQ */
  Fragrecord *regFragPtr = prepare_fragptr.p;

  req_struct.trans_id1 = transId1;
  req_struct.trans_id2 = transId2;
  req_struct.tablePtrP = regTabPtr;
  req_struct.fragPtrP = regFragPtr;

  const Uint32 Roptype = regOperPtr->op_type;

  regOperPtr->m_any_value = 0;
  const Uint32 loc_prepare_page_id = prepare_page_no;
  /**
   * Check operation
   */
  if (likely(Roptype == ZREAD))
  {
    jamDebug();
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 0;
    ndbassert(!Local_key::isInvalid(pageid, pageidx));

    if (unlikely(isCopyTuple(pageid, pageidx)))
    {
      jamDebug();
      /**
       * Only LCP reads a copy-tuple "directly"
       */
      ndbassert(disk_page == RNIL);
      ndbassert(!m_is_query_block);
      setup_lcp_read_copy_tuple(&req_struct, regOperPtr, regTabPtr);
    }
    else
    {
      /**
       * Get pointer to tuple
       */
      jamDebug();
      regOperPtr->m_tuple_location.m_page_no = loc_prepare_page_id;
      setup_fixed_tuple_ref_opt(&req_struct);
      setup_fixed_part(&req_struct, regOperPtr, regTabPtr);
      /**
       * When coming here as a Query thread we must grab a mutex to ensure
       * that the row version we see is written properly, once we have
       * retrieved the row version we need no more protection since the
       * next change either comes through an ABORT or a COMMIT operation
       * and these are all exclusive access that first will ensure that no
       * query threads are executing on the fragment before proceeding.
       */
      acquire_frag_mutex_read(regFragPtr, pageid, jamBuffer());
      if (unlikely(req_struct.m_tuple_ptr->m_header_bits &
            Tuple_header::FREE))
      {
        jam();
        terrorCode = ZTUPLE_DELETED_ERROR;
        tupkeyErrorLab(&req_struct);
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        return false;
      }
      if (unlikely(setup_read(&req_struct, regOperPtr, regTabPtr, 
              disk_page != RNIL) == false))
      {
        jam();
        tupkeyErrorLab(&req_struct);
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        return false;
      }
      /* Check checksum with mutex protection. */
      if (unlikely(((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
              (calculateChecksum(req_struct.m_tuple_ptr, regTabPtr) != 0)) ||
            ERROR_INSERTED(4036)))
      {
        jam();
        release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
        corruptedTupleDetected(&req_struct, regTabPtr);
        return false;
      }
      release_frag_mutex_read(regFragPtr, pageid, jamBuffer());
    }
    if (handleReadReq(signal, regOperPtr, regTabPtr, &req_struct) != -1)
    {
      req_struct.log_size= 0;
      /* ---------------------------------------------------------------- */
      // Read Operations need not to be taken out of any lists. 
      // We also do not need to wait for commit since there is no changes 
      // to commit. Thus we
      // prepare the operation record already now for the next operation.
      // Write operations set the state to STARTED indicating that they
      // are waiting for the Commit or Abort decision.
      /* ---------------------------------------------------------------- */
      /**
       * We could release fragment access here for read key readers, but not
       * for scan operations.
       */
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_IDLE);
      return true;
    }
    jamDebug();
    return false;
  }
  /**
   * DBQTUP can come here when executing restore, but query thread should
   * not arrive here.
   */
  ndbassert(!m_is_in_query_thread);
  req_struct.changeMask.clear();
  Tuple_header *tuple_ptr = nullptr;

  if (!Local_key::isInvalid(pageid, pageidx))
  {
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 1;
  }
  else
  {
    regOperPtr->op_struct.bit_field.in_active_list = false;
    regOperPtr->op_struct.bit_field.m_tuple_existed_at_start = 0;
    req_struct.prevOpPtr.i = RNIL;
    if (Roptype == ZINSERT)
    {
      // No tuple allocated yet
      jamDebug();
      goto do_insert;
    }
    if (Roptype == ZREFRESH)
    {
      // No tuple allocated yet
      jamDebug();
      goto do_refresh;
    }
    ndbabort();
  }
  ndbassert(!isCopyTuple(pageid, pageidx));
  /**
   * Get pointer to tuple
   */
  regOperPtr->m_tuple_location.m_page_no = loc_prepare_page_id;
  setup_fixed_tuple_ref_opt(&req_struct);
  setup_fixed_part(&req_struct, regOperPtr, regTabPtr);
  tuple_ptr = req_struct.m_tuple_ptr;

  if (prepareActiveOpList(operPtr, &req_struct))
  {
    m_base_header_bits = tuple_ptr->m_header_bits;
    if(Roptype == ZINSERT)
    {
      jam();
do_insert:
      Local_key accminupdate;
      Local_key * accminupdateptr = &accminupdate;
      if (unlikely(handleInsertReq(signal,
              operPtr,
              prepare_fragptr,
              regTabPtr,
              &req_struct,
              &accminupdateptr,
              false) == -1))
      {
        return false;
      }

      if (tuple_ptr != nullptr)
      {
        jam();
        acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
        /**
         * Updates of checksum needs to be protected during non-initial
         * INSERTs.
         */
        if (tuple_ptr->m_header_bits != m_base_header_bits)
        {
          /**
           * The checksum is invalid if the ALLOC flag is set in the
           * header bits, but there is no problem in recalculating a
           * new incorrect checksum. So we will perform this calculation
           * even when it isn't required to do it.
           *
           * The bits must still be updated as other threads can look
           * at some bits even before checksum has been set.
           * Updating header bits need always be protected by the TUP
           * fragment mutex.
           */
          Uint32 old_header = tuple_ptr->m_header_bits;
          tuple_ptr->m_header_bits = m_base_header_bits;
          updateChecksum(tuple_ptr,
              regTabPtr,
              old_header,
              tuple_ptr->m_header_bits);
        }

#if defined(VM_TRACE) || defined(ERROR_INSERT)
        /**
         * Verify that we didn't mess up the checksum
         * If the ALLOC flag is set it means that the row hasn't been
         * committed yet, in this state the checksum isn't yet properly
         * set. Thus it makes no sense to verify it.
         */
        if (tuple_ptr != nullptr &&
            ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
            (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
            (calculateChecksum(tuple_ptr, regTabPtr) != 0))
        {
          ndbabort();
        }
#endif
        /**
         * Prepare of INSERT operations is different dependent on whether the
         * row existed before or not (it can exist before if we had a DELETE
         * operation before it in the same transaction). If the row didn't
         * exist then no one can see the row until we have filled in the
         * local key in DBACC which happens below in the call to accminupdate.
         *
         * If the row existed before we need to grab a mutex to ensure that
         * concurrent key readers see a consistent view of the row. We need
         * to update the row before we execute the TUX triggers since they
         * make use of the linked list of operations on the row and this
         * needs to be visible when executing the prepare insert triggers
         * on the TUX index.
         *
         * The INSERT is made visible to other read operations through the
         * call to insertActiveOpList, this includes making it visible to
         * trigger code. If the INSERT is aborted, the inserted row will
         * be visible to read operations from the same transaction for a
         * short time, but first of all reading rows concurrently with an
         * INSERT does not deliver guaranteed results in the first place
         * and second if the transaction aborts, it should not consider
         * the read value anyways. So it should be safe to release the
         * mutex and make the new row visible immediately after
         * completing the INSERT operation and before the actual trigger
         * execution happens that in a rare case could cause the operation
         * to be aborted.
         */
        insertActiveOpList(operPtr, &req_struct, tuple_ptr);
        release_frag_mutex(regFragPtr, pageid, jamBuffer());
      }
      else
      {
        /**
         * An initial INSERT operation requires no mutex, and it is
         * trivially already in the active list, even the flag is set
         * in the handleInsertReq method. The insert operation
         * is made visible through the call to execACCMINUPDATE later.
         */
        jam();
      }
      terrorCode = 0;
      checkImmediateTriggersAfterInsert(&req_struct,
          regOperPtr,
          regTabPtr,
          disk_page != RNIL);

      if (likely(terrorCode == 0))
      {
        if (!regTabPtr->tuxCustomTriggers.isEmpty()) 
        {
          jam();
          /**
           * Ensure that no concurrent scans happens while I am
           * updating the TUX indexes.
           *
           * It is vital that I don't hold any fragment mutex while making
           * this call since that could cause a deadlock if any of the
           * threads I am waiting on is requiring this lock to be able to
           * complete its operation before allowing write key access.
           */
          c_lqh->upgrade_to_write_key_frag_access();
          executeTuxInsertTriggers(signal,
              regOperPtr,
              regFragPtr,
              regTabPtr);
        }
      }

      if (unlikely(terrorCode != 0))
      {
        jam();
        /*
         * TUP insert succeeded but immediate trigger firing or
         * add of TUX entries failed.  
         * All TUX changes have been rolled back at this point.
         *
         * We will abort via tupkeyErrorLab() as usual.  This routine
         * however resets the operation to ZREAD.  The TUP_ABORTREQ
         * arriving later cannot then undo the insert.
         *
         * Therefore we call TUP_ABORTREQ already now.  Diskdata etc
         * should be in memory and timeslicing cannot occur.  We must
         * skip TUX abort triggers since TUX is already aborted.  We
         * will dealloc the fixed and var parts if necessary.
         */
        c_lqh->upgrade_to_exclusive_frag_access_no_return();
        signal->theData[0] = operPtr.i;
        do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS | ZABORT_DEALLOC);
        tupkeyErrorLab(&req_struct);
        return false;
      }
      /**
       * It is ok to release fragment access already here since the
       * call to ACCMINUPDATE will make the new row appear to other operations
       * in the same transaction, but this is protected by the ACC fragment
       * mutex and requires no special access to the table fragment. TUX
       * index readers get access to the row by the above call to
       * executeTuxInsertTriggers. Thus scanners get access to the new row
       * slightly ahead of read key readers, but this only matters for the
       * operations within the same transaction and we don't guarantee order
       * of those operations towards each other anyways.
       */
      c_lqh->release_frag_access();
      if (accminupdateptr)
      {
        /**
         * Update ACC local-key, once *everything* has completed successfully
         */
        c_lqh->accminupdate(signal,
            regOperPtr->userpointer,
            accminupdateptr);
      }
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    }

    ndbassert(!(req_struct.m_tuple_ptr->m_header_bits & Tuple_header::FREE));

    if (Roptype == ZUPDATE) {
      jamDebug();
      if (unlikely(handleUpdateReq(signal, regOperPtr,
              regFragPtr, regTabPtr,
              &req_struct, disk_page != RNIL) == -1))
      {
        return false;
      }
      /**
       * The lock on the TUP fragment is required to update header info on
       * the base row, thus we use the variable m_base_header_bits in
       * handleUpdateReq and postpone updating the checksum under mutex
       * protection until after completing the call to handleUpdateReq.
       * This shortens the time we hold the mutex on the fragment part this
       * row belongs to.
       *
       * We can execute other key reads from the query thread concurrently,
       * thus we need to acquire a mutex while inserting the operation
       * into the linked list of operations on the row.
       *
       * Scans will not run in parallel with parallel updates. So the lock
       * on triggers is since we need to set the operation record in the
       * row header before executing the triggers. There is no need for the
       * lock though during execution of the immediate triggers.
       */
      jamDebug();
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      if (tuple_ptr->m_header_bits != m_base_header_bits)
      {
        jamDebug();
        Uint32 old_header = tuple_ptr->m_header_bits;
        tuple_ptr->m_header_bits = m_base_header_bits;
        updateChecksum(tuple_ptr,
            regTabPtr,
            old_header,
            tuple_ptr->m_header_bits);
      }
      insertActiveOpList(operPtr, &req_struct, tuple_ptr);
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0))
      {
        ndbabort();
      }
#endif
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
      terrorCode = 0;
      checkImmediateTriggersAfterUpdate(&req_struct,
          regOperPtr,
          regTabPtr,
          disk_page != RNIL);

      if (unlikely(terrorCode != 0))
      {
        tupkeyErrorLab(&req_struct);
        return false;
      }

      if (!regTabPtr->tuxCustomTriggers.isEmpty())
      {
        jam();
        c_lqh->upgrade_to_write_key_frag_access();
        if (unlikely(executeTuxUpdateTriggers(signal,
                regOperPtr,
                regFragPtr,
                regTabPtr) != 0))
        {
          jam();
          /*
           * See insert case.
           */
          c_lqh->upgrade_to_exclusive_frag_access_no_return();
          signal->theData[0] = operPtr.i;
          do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS);
          tupkeyErrorLab(&req_struct);
          return false;
        }
      }
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    }
    else if(Roptype == ZDELETE)
    {
      jam();
      req_struct.log_size= 0;
      if (unlikely(handleDeleteReq(signal, regOperPtr,
              regFragPtr, regTabPtr,
              &req_struct,
              disk_page != RNIL) == -1))
      {
        return false;
      }

      terrorCode = 0;
      /**
       * Prepare of DELETE operations only use shared access to fragments,
       * thus we need to insert the DELETE operation into the list of
       * of operations in a safe way to ensure that there is a well defined
       * point where READ operations can see this row version.
       *
       * It is important to also hold mutex while calling tupkeyErrorLab in
       * case something goes wrong in checking triggers, this ensures that
       * we remove the tuple from the view of the readers before they get
       * access to it.
       */
      jamDebug();
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      insertActiveOpList(operPtr, &req_struct, tuple_ptr);
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
      checkImmediateTriggersAfterDelete(&req_struct,
          regOperPtr,
          regTabPtr,
          disk_page != RNIL);

      if (unlikely(terrorCode != 0))
      {
        tupkeyErrorLab(&req_struct);
        return false;
      }
      /*
       * TUX doesn't need to check for triggers at delete since entries in
       * the index are kept until commit time.
       */
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      acquire_frag_mutex(regFragPtr, pageid, jamBuffer());
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0))
      {
        release_frag_mutex(regFragPtr, pageid, jamBuffer());
        ndbabort();
      }
      release_frag_mutex(regFragPtr, pageid, jamBuffer());
#endif
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    }
    else if (Roptype == ZREFRESH)
    {
      /**
       * No TUX or immediate triggers, just detached triggers
       */
do_refresh:
      jamDebug();
      c_lqh->upgrade_to_exclusive_frag_access_no_return();
      if (unlikely(handleRefreshReq(signal, operPtr,
              prepare_fragptr, regTabPtr,
              &req_struct, disk_page != RNIL) == -1))
      {
        return false;
      }
      if (tuple_ptr)
      {
        jam();
        insertActiveOpList(operPtr, &req_struct, tuple_ptr);
      }
      else
      {
        jam();
        operPtr.p->op_struct.bit_field.in_active_list = true;
      }
#if defined(VM_TRACE) || defined(ERROR_INSERT)
      /* Verify that we didn't mess up the checksum */
      if (tuple_ptr != nullptr &&
          ((tuple_ptr->m_header_bits & Tuple_header::ALLOC) == 0) &&
          (regTabPtr->m_bits & Tablerec::TR_Checksum) &&
          (calculateChecksum(tuple_ptr, regTabPtr) != 0))
      {
        ndbabort();
      }
#endif
      c_lqh->release_frag_access();
      returnTUPKEYCONF(signal, &req_struct, regOperPtr, TRANS_STARTED);
      return true;
    }
    else
    {
      ndbabort(); // Invalid op type
    }
  }
  tupkeyErrorLab(&req_struct);
  return false;
}

void
Dbtup::setup_fixed_part(KeyReqStruct* req_struct,
                        Operationrec* regOperPtr,
                        Tablerec* regTabPtr)
{
  ndbassert(regOperPtr->op_type == ZINSERT ||
      (! (req_struct->m_tuple_ptr->m_header_bits & Tuple_header::FREE)));

  Uint32* tab_descr = regTabPtr->tabDescriptor;
  Uint32 mm_check_offset = regTabPtr->get_check_offset(MM);
  Uint32 dd_check_offset = regTabPtr->get_check_offset(DD);

  req_struct->check_offset[MM] = mm_check_offset;
  req_struct->check_offset[DD] = dd_check_offset;
  req_struct->attr_descr = tab_descr;
  NDB_PREFETCH_READ((char*)tab_descr);
}

void
Dbtup::setup_lcp_read_copy_tuple(KeyReqStruct* req_struct,
                                 Operationrec* regOperPtr,
                                 Tablerec* regTabPtr)
{
  Local_key tmp;
  tmp.m_page_no = req_struct->frag_page_id;
  tmp.m_page_idx = regOperPtr->m_tuple_location.m_page_idx;
  clearCopyTuple(tmp.m_page_no, tmp.m_page_idx);

  Uint32 * copytuple = get_copy_tuple_raw(&tmp);
  Local_key rowid;
  memcpy(&rowid, copytuple+0, sizeof(Local_key));

  req_struct->frag_page_id = rowid.m_page_no;
  regOperPtr->m_tuple_location.m_page_idx = rowid.m_page_idx;

  Uint32* tab_descr = regTabPtr->tabDescriptor;
  Tuple_header * th = get_copy_tuple(copytuple);
  req_struct->m_page_ptr.setNull();
  req_struct->m_tuple_ptr = (Tuple_header*)th;
  th->m_operation_ptr_i = RNIL;
  ndbassert((th->m_header_bits & Tuple_header::COPY_TUPLE) != 0);

  req_struct->attr_descr= tab_descr;

  bool disk = false;
  if (regTabPtr->need_expand(disk))
  {
    jam();
    prepare_read(req_struct, regTabPtr, disk);
  }
}

/* ---------------------------------------------------------------- */
/* ------------------------ CONFIRM REQUEST ----------------------- */
/* ---------------------------------------------------------------- */
inline
void Dbtup::returnTUPKEYCONF(Signal* signal,
                             KeyReqStruct *req_struct,
                             Operationrec * regOperPtr,
                             TransState trans_state)
{
  /**
   * When we arrive here we have been executing read code and/or write
   * code to read/write the tuple. During this execution path we have
   * not accessed the regOperPtr object for a long time and we have
   * accessed lots of other data in the meantime. This prefetch was
   * shown useful by using the perf tool. So not an obvious prefetch.
   */
  NDB_PREFETCH_WRITE(regOperPtr);
  TupKeyConf * tupKeyConf= (TupKeyConf *)signal->getDataPtrSend();  

  Uint32 Rcreate_rowid = req_struct->m_use_rowid;
  Uint32 RuserPointer= regOperPtr->userpointer;
  Uint32 RnumFiredTriggers= req_struct->num_fired_triggers;
  const Uint32 RnoExecInstructions = req_struct->no_exec_instructions;
  Uint32 log_size= req_struct->log_size;
  Uint32 read_length= req_struct->read_length;
  Uint32 last_row= req_struct->last_row;

  tupKeyConf->userPtr= RuserPointer;
  tupKeyConf->readLength= read_length;
  tupKeyConf->writeLength= log_size;
  tupKeyConf->numFiredTriggers= RnumFiredTriggers;
  tupKeyConf->lastRow= last_row;
  tupKeyConf->rowid = Rcreate_rowid;
  tupKeyConf->noExecInstructions = RnoExecInstructions;
  set_tuple_state(regOperPtr, TUPLE_PREPARED);
  set_trans_state(regOperPtr, trans_state);
}


#define MAX_READ (MIN(sizeof(signal->theData), MAX_SEND_MESSAGE_BYTESIZE))

/* ---------------------------------------------------------------- */
/* ----------------------------- READ  ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleReadReq(Signal* signal,
                         // UNUSED, is member of req_struct
                         Operationrec* _regOperPtr,
                         Tablerec* regTabPtr,
                         KeyReqStruct* req_struct)
{
  Uint32 *dst;
  Uint32 dstLen, start_index;
  const BlockReference sendBref= req_struct->rec_blockref;
  const Uint32 node = refToNode(sendBref);
  if(node != 0 && node != getOwnNodeId())
  {
    start_index= 25;
  }
  else
  {
    jamDebug();
    /**
     * execute direct
     */
    start_index= AttrInfo::HeaderLength;  //3;
  }
  dst= &signal->theData[start_index];
  dstLen= (MAX_READ / 4) - start_index;
  if (!req_struct->interpreted_exec)
  {
    jamDebug();
    int ret = readAttributes(req_struct,
                             &cinBuffer[0],
                             req_struct->attrinfo_len,
                             dst,
                             dstLen);
    if (likely(ret >= 0))
    {
      /* ------------------------------------------------------------------------- */
      // We have read all data into coutBuffer. Now send it to the API.
      /* ------------------------------------------------------------------------- */
      jamDebug();
      const Uint32 TnoOfDataRead= (Uint32) ret;
      sendReadAttrinfo(signal, req_struct, TnoOfDataRead);
      return 0;
    }
    else
    {
      terrorCode = Uint32(-ret);
    }
  }
  else
  {
    return interpreterStartLab(signal, req_struct);
  }

  jam();
  tupkeyErrorLab(req_struct);
  return -1;
}

static
void
handle_reorg(Dbtup::KeyReqStruct * req_struct,
             Dbtup::Fragrecord::FragState state)
{
  Uint32 reorg = req_struct->m_reorg;
  switch(state){
    case Dbtup::Fragrecord::FS_FREE:
    case Dbtup::Fragrecord::FS_REORG_NEW:
    case Dbtup::Fragrecord::FS_REORG_COMMIT_NEW:
    case Dbtup::Fragrecord::FS_REORG_COMPLETE_NEW:
      return;
    case Dbtup::Fragrecord::FS_REORG_COMMIT:
    case Dbtup::Fragrecord::FS_REORG_COMPLETE:
      if (reorg != ScanFragReq::REORG_NOT_MOVED)
        return;
      break;
    case Dbtup::Fragrecord::FS_ONLINE:
      if (reorg != ScanFragReq::REORG_MOVED)
        return;
      break;
    default:
      return;
  }
  req_struct->m_tuple_ptr->m_header_bits |= Dbtup::Tuple_header::REORG_MOVE;
}

/* ---------------------------------------------------------------- */
/* ---------------------------- UPDATE ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleUpdateReq(Signal* signal,
                           Operationrec* operPtrP,
                           Fragrecord* regFragPtr,
                           Tablerec* regTabPtr,
                           KeyReqStruct* req_struct,
                           bool disk) 
{
  Tuple_header *dst;
  Tuple_header *base= req_struct->m_tuple_ptr, *org;
  ChangeMask * change_mask_ptr;
  if (unlikely((dst= alloc_copy_tuple(regTabPtr,
            &operPtrP->m_copy_tuple_location)) == 0))
  {
    terrorCode= ZNO_COPY_TUPLE_MEMORY_ERROR;
    goto error;
  }

  Uint32 tup_version;
  change_mask_ptr = get_change_mask_ptr(regTabPtr, dst);
  if (likely(operPtrP->is_first_operation()))
  {
    jamDebug();
    org= req_struct->m_tuple_ptr;
    tup_version= org->get_tuple_version();
    clear_change_mask_info(regTabPtr, change_mask_ptr);
  }
  else
  {
    jam();
    Operationrec* prevOp= req_struct->prevOpPtr.p;
    tup_version= prevOp->op_struct.bit_field.tupVersion;
    Uint32 * rawptr = get_copy_tuple_raw(&prevOp->m_copy_tuple_location);
    org= get_copy_tuple(rawptr);
    copy_change_mask_info(regTabPtr,
        change_mask_ptr,
        get_change_mask_ptr(rawptr));
  }

  /**
   * Check consistency before update/delete
   * Any updates made to the row and checksum is performed by the LDM
   * thread and thus protected by being in the same thread.
   */
  req_struct->m_tuple_ptr= org;
  if (unlikely((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
        (calculateChecksum(req_struct->m_tuple_ptr, regTabPtr) != 0)))
  {
    jam();
    return corruptedTupleDetected(req_struct, regTabPtr);
  }

  req_struct->m_tuple_ptr= dst;
  union {
    Uint32 sizes[4];
    Uint64 cmp[2];
  };

  disk = disk || (org->m_header_bits & Tuple_header::DISK_INLINE);
  if (regTabPtr->need_expand(disk))
  {
    jamDebug();
    expand_tuple(req_struct, sizes, org, regTabPtr, disk);
    if (disk && operPtrP->m_undo_buffer_space == 0)
    {
      jam();
      operPtrP->op_struct.bit_field.m_wait_log_buffer = 1;
      operPtrP->op_struct.bit_field.m_load_diskpage_on_commit = 1;
      operPtrP->m_undo_buffer_space= 
        (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + sizes[DD] - 1;
      jamDataDebug(operPtrP->m_undo_buffer_space);

      {
        D("Logfile_client - handleUpdateReq");
        Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
        DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
              instance(),
              __LINE__,
              operPtrP->m_undo_buffer_space));
        terrorCode= lgman.alloc_log_space(operPtrP->m_undo_buffer_space,
            true,
            !req_struct->m_nr_copy_or_redo,
            jamBuffer());
      }
      if(unlikely(terrorCode))
      {
        jam();
        operPtrP->m_undo_buffer_space= 0;
        goto error;
      }
    }
  }
  else
  {
    memcpy(dst, org, 4*regTabPtr->m_offsets[MM].m_fix_header_size);
    req_struct->m_tuple_ptr->m_header_bits |= Tuple_header::COPY_TUPLE;
  }

  tup_version= (tup_version + 1) & ZTUP_VERSION_MASK;
  operPtrP->op_struct.bit_field.tupVersion= tup_version;

  req_struct->optimize_options = 0;

  if (!req_struct->interpreted_exec)
  {
    jamDebug();

    if (unlikely(regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits))
    {
      jam();
      Uint32 attrId =
        regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

      store_extra_row_bits(attrId, regTabPtr, dst, /* default */ 0, false);
    }
    int retValue = updateAttributes(req_struct,
        &cinBuffer[0],
        req_struct->attrinfo_len);
    if (unlikely(retValue < 0))
    {
      terrorCode = Uint32(-retValue);
      goto error;
    }
  }
  else
  {
    if (unlikely(interpreterStartLab(signal, req_struct) == -1))
      return -1;
  }

  update_change_mask_info(regTabPtr,
      change_mask_ptr,
      req_struct->changeMask.rep.data);

  switch (req_struct->optimize_options) {
    case AttributeHeader::OPTIMIZE_MOVE_VARPART:
      /**
       * optimize varpart of tuple,  move varpart of tuple from
       * big-free-size page list into small-free-size page list
       */
      if(base->m_header_bits & Tuple_header::VAR_PART)
      {
        jam();
        optimize_var_part(req_struct, base, operPtrP,
            regFragPtr, regTabPtr);
      }
      break;
    case AttributeHeader::OPTIMIZE_MOVE_FIXPART:
      //TODO: move fix part of tuple
      break;
    default:
      break;
  }

  if (regTabPtr->need_shrink())
  {
    jamDebug();
    shrink_tuple(req_struct, sizes+2, regTabPtr, disk);
    if (cmp[0] != cmp[1] &&
        ((handle_size_change_after_update(signal,
                                          req_struct,
                                          base,
                                          operPtrP,
                                          regFragPtr,
                                          regTabPtr,
                                          sizes)) != 0))
    {
      goto error;
    }
  }

  if (req_struct->m_reorg != ScanFragReq::REORG_ALL)
  {
    handle_reorg(req_struct, regFragPtr->fragStatus);
  }

  req_struct->m_tuple_ptr->set_tuple_version(tup_version);

  /**
   * We haven't made the tuple available for readers yet, so no need
   * to protect this change.
   */
  setChecksum(req_struct->m_tuple_ptr, regTabPtr);
  set_tuple_state(operPtrP, TUPLE_PREPARED);

  return 0;

error:
  tupkeyErrorLab(req_struct);
  return -1;
}

/*
   expand_dyn_part - copy dynamic attributes to fully expanded size.

   Both variable-sized and fixed-size attributes are stored in the same way
   in the expanded form as variable-sized attributes (in expand_var_part()).

   This method is used for both mem and disk dynamic data.

   dst         Destination for expanded data
   tabPtrP     Table descriptor
   src         Pointer to the start of dynamic bitmap in source row
   row_len     Total number of 32-bit words in dynamic part of row
   tabDesc     Array of table descriptors
   order       Array of indexes into tabDesc, dynfix followed by dynvar
 */
static
Uint32*
expand_dyn_part(Dbtup::KeyReqStruct::Var_data *dst,
                const Uint32* src,
                Uint32 row_len,
                const Uint32 * tabDesc,
                const Uint16* order,
                Uint32 dynvar,
                Uint32 dynfix,
                Uint32 max_bmlen)
{
  /* Copy the bitmap, zeroing out any words not stored in the row. */
  Uint32 *dst_bm_ptr= (Uint32*)dst->m_dyn_data_ptr;
  Uint32 bm_len = row_len ? (* src & Dbtup::DYN_BM_LEN_MASK) : 0;

  assert(bm_len <= max_bmlen);

  if(bm_len > 0)
    memcpy(dst_bm_ptr, src, 4*bm_len);
  if(bm_len < max_bmlen)
    std::memset(dst_bm_ptr + bm_len, 0, 4 * (max_bmlen - bm_len));

  /**
   * Store max_bmlen for homogenous code in DbtupRoutines
   */
  Uint32 tmp = (* dst_bm_ptr);
  * dst_bm_ptr = (tmp & ~(Uint32)Dbtup::DYN_BM_LEN_MASK) | max_bmlen;

  char *src_off_start= (char*)(src + bm_len);
  assert((UintPtr(src_off_start)&3) == 0);
  Uint16 *src_off_ptr= (Uint16*)src_off_start;

  /*
     Prepare the variable-sized dynamic attributes, copying out data from the
     source row for any that are not NULL.
   */
  Uint32 no_attr= dst->m_dyn_len_offset;
  Uint16* dst_off_ptr= dst->m_dyn_offset_arr_ptr;
  Uint16* dst_len_ptr= dst_off_ptr + no_attr;
  Uint16 this_src_off= row_len ? * src_off_ptr++ : 0;
  /* We need to reserve room for the offsets written by shrink_tuple+padding.*/
  Uint16 dst_off= 4 * (max_bmlen + ((dynvar+2)>>1));
  char *dst_ptr= (char*)dst_bm_ptr + dst_off;
  for(Uint32 i= 0; i<dynvar; i++)
  {
    Uint16 j= order[dynfix+i];
    Uint32 max_len= 4 *AttributeDescriptor::getSizeInWords(tabDesc[j]);
    Uint32 len;
    Uint32 pos = AttributeOffset::getNullFlagPos(tabDesc[j+1]);
    if(bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, src, pos))
    {
      Uint16 next_src_off= *src_off_ptr++;
      len= next_src_off - this_src_off;
      memcpy(dst_ptr, src_off_start+this_src_off, len);
      this_src_off= next_src_off;
    }
    else
    {
      len= 0;
    }
    dst_off_ptr[i]= dst_off;
    dst_len_ptr[i]= dst_off+len;
    dst_off+= max_len;
    dst_ptr+= max_len;
  }
  /*
     The fixed-size data is stored 32-bit aligned after the variable-sized
     data.
   */
  char *src_ptr= src_off_start+this_src_off;
  src_ptr= (char *)(ALIGN_WORD(src_ptr));

  /*
     Prepare the fixed-size dynamic attributes, copying out data from the
     source row for any that are not NULL.
     Note that the fixed-size data is stored in reverse from the end of the
     dynamic part of the row. This is true both for the stored/shrunken and
     for the expanded form.
   */
  for(Uint32 i= dynfix; i>0; )
  {
    i--;
    Uint16 j= order[i];
    Uint32 fix_size= 4*AttributeDescriptor::getSizeInWords(tabDesc[j]);
    dst_off_ptr[dynvar+i]= dst_off;
    /* len offset array is not used for fixed size. */
    Uint32 pos = AttributeOffset::getNullFlagPos(tabDesc[j+1]);
    if(bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, src, pos))
    {
      assert((UintPtr(dst_ptr)&3) == 0);
      memcpy(dst_ptr, src_ptr, fix_size);
      src_ptr+= fix_size;
    }
    dst_off+= fix_size;
    dst_ptr+= fix_size;
  }

  return (Uint32 *)dst_ptr;
}

static
Uint32*
shrink_dyn_part(Dbtup::KeyReqStruct::Var_data *dst,
                Uint32 *dst_ptr,
                const Dbtup::Tablerec* tabPtrP,
                const Uint32 * tabDesc,
                const Uint16* order,
                Uint32 dynvar,
                Uint32 dynfix,
                Uint32 ind)
{
  /**
   * Now build the dynamic part, if any.
   * First look for any trailing all-NULL words of the bitmap; we do
   * not need to store those.
   */
  assert((UintPtr(dst->m_dyn_data_ptr)&3) == 0);
  char *dyn_src_ptr= dst->m_dyn_data_ptr;
  Uint32 bm_len = tabPtrP->m_offsets[ind].m_dyn_null_words; // In words

  /* If no dynamic variables, store nothing. */
  assert(bm_len);
  {
    /**
     * clear bm-len bits, so they won't incorrect indicate
     *   a non-zero map
     */
    * ((Uint32 *)dyn_src_ptr) &= ~Uint32(Dbtup::DYN_BM_LEN_MASK);

    Uint32 *bm_ptr= (Uint32 *)dyn_src_ptr + bm_len - 1;
    while(*bm_ptr == 0)
    {
      bm_ptr--;
      bm_len--;
      if(bm_len == 0)
        break;
    }
  }

  if (bm_len)
  {
    /**
     * Copy the bitmap, counting the number of variable sized
     * attributes that are not NULL on the way.
     */
    Uint32 *dyn_dst_ptr= dst_ptr;
    Uint32 dyn_var_count= 0;
    const Uint32 *src_bm_ptr= (Uint32 *)(dyn_src_ptr);
    Uint32 *dst_bm_ptr= (Uint32 *)dyn_dst_ptr;

    /* ToDo: Put all of the dynattr code inside if(bm_len>0) { ... },
     * split to separate function. */
    Uint16 dyn_dst_data_offset= 0;
    const Uint32 *dyn_bm_var_mask_ptr= tabPtrP->dynVarSizeMask[ind];
    for(Uint16 i= 0; i< bm_len; i++)
    {
      Uint32 v= src_bm_ptr[i];
      dyn_var_count+= BitmaskImpl::count_bits(v & *dyn_bm_var_mask_ptr++);
      dst_bm_ptr[i]= v;
    }

    Uint32 tmp = *dyn_dst_ptr;
    assert(bm_len <= Dbtup::DYN_BM_LEN_MASK);
    * dyn_dst_ptr = (tmp & ~(Uint32)Dbtup::DYN_BM_LEN_MASK) | bm_len;
    dyn_dst_ptr+= bm_len;
    dyn_dst_data_offset= 2*dyn_var_count + 2;

    Uint16 *dyn_src_off_array= dst->m_dyn_offset_arr_ptr;
    Uint16 *dyn_src_lenoff_array=
      dyn_src_off_array + dst->m_dyn_len_offset;
    Uint16* dyn_dst_off_array = (Uint16*)dyn_dst_ptr;

    /**
     * Copy over the variable sized not-NULL attributes.
     * Data offsets are counted from the start of the offset array, and
     * we store one additional offset to be able to easily compute the
     * data length as the difference between offsets.
     */
    Uint16 off_idx= 0;
    for(Uint32 i= 0; i<dynvar; i++)
    {
      /**
       * Note that we must use the destination (shrunken) bitmap here,
       * as the source (expanded) bitmap may have been already clobbered
       * (by offset data).
       */
      Uint32 attrDesc2 = tabDesc[order[dynfix+i]+1];
      Uint32 pos = AttributeOffset::getNullFlagPos(attrDesc2);
      if (bm_len > (pos >> 5) && BitmaskImpl::get(bm_len, dst_bm_ptr, pos))
      {
        dyn_dst_off_array[off_idx++]= dyn_dst_data_offset;
        Uint32 dyn_src_off= dyn_src_off_array[i];
        Uint32 dyn_len= dyn_src_lenoff_array[i] - dyn_src_off;
        memmove(((char *)dyn_dst_ptr) + dyn_dst_data_offset,
            dyn_src_ptr + dyn_src_off,
            dyn_len);
        dyn_dst_data_offset+= dyn_len;
      }
    }
    /* If all dynamic attributes are NULL, we store nothing. */
    dyn_dst_off_array[off_idx]= dyn_dst_data_offset;
    assert(dyn_dst_off_array + off_idx == (Uint16*)dyn_dst_ptr+dyn_var_count);

    char *dynvar_end_ptr= ((char *)dyn_dst_ptr) + dyn_dst_data_offset;
    char *dyn_dst_data_ptr= (char *)(ALIGN_WORD(dynvar_end_ptr));

    /**
     * Zero out any padding bytes. Might not be strictly necessary,
     * but seems cleaner than leaving random stuff in there.
     */
    std::memset(dynvar_end_ptr, 0, dyn_dst_data_ptr-dynvar_end_ptr);

    /* *
     * Copy over the fixed-sized not-NULL attributes.
     * Note that attributes are copied in reverse order; this is to avoid
     * overwriting not-yet-copied data, as the data is also stored in
     * reverse order.
     */
    for(Uint32 i= dynfix; i > 0; )
    {
      i--;
      Uint16 j= order[i];
      Uint32 attrDesc2 = tabDesc[j+1];
      Uint32 pos = AttributeOffset::getNullFlagPos(attrDesc2);
      if(bm_len > (pos >>5 ) && BitmaskImpl::get(bm_len, dst_bm_ptr, pos))
      {
        Uint32 fixsize=
          4*AttributeDescriptor::getSizeInWords(tabDesc[j]);
        memmove(dyn_dst_data_ptr,
            dyn_src_ptr + dyn_src_off_array[dynvar+i],
            fixsize);
        dyn_dst_data_ptr += fixsize;
      }
    }
    dst_ptr = (Uint32*)dyn_dst_data_ptr;
    assert((UintPtr(dst_ptr) & 3) == 0);
  }
  return (Uint32 *)dst_ptr;
}

/* ---------------------------------------------------------------- */
/* ----------------------------- INSERT --------------------------- */
/* ---------------------------------------------------------------- */
void
Dbtup::prepare_initial_insert(KeyReqStruct *req_struct, 
                              Operationrec* regOperPtr,
                              Tablerec* regTabPtr,
                              bool is_refresh)
{
  Uint32 disk_undo = ((regTabPtr->m_no_of_disk_attributes > 0) &&
      !is_refresh) ? 
    sizeof(Dbtup::Disk_undo::Alloc) >> 2 : 0;
  regOperPtr->m_undo_buffer_space= disk_undo;
  jamDebug();
  jamDataDebug(regOperPtr->m_undo_buffer_space);

  req_struct->check_offset[MM]= regTabPtr->get_check_offset(MM);
  req_struct->check_offset[DD]= regTabPtr->get_check_offset(DD);

  Uint16* order = regTabPtr->m_real_order_descriptor;
  Uint32 *tab_descr = regTabPtr->tabDescriptor;
  req_struct->attr_descr = tab_descr; 

  Uint32 bits = Tuple_header::COPY_TUPLE;
  bits |= disk_undo ? (Tuple_header::DISK_ALLOC|Tuple_header::DISK_INLINE) : 0;
  req_struct->m_tuple_ptr->m_header_bits= bits;

  Uint32 *ptr= req_struct->m_tuple_ptr->get_end_of_fix_part_ptr(regTabPtr);
  Var_part_ref* ref = req_struct->m_tuple_ptr->get_var_part_ref_ptr(regTabPtr);

  if (regTabPtr->m_bits & Tablerec::TR_ForceVarPart)
  {
    ref->m_page_no = RNIL; 
    ref->m_page_idx = Tup_varsize_page::END_OF_FREE_LIST;
  }

  for (Uint32 ind = 0; ind < 2; ind++)
  {
    const Uint32 num_fix = regTabPtr->m_attributes[ind].m_no_of_fixsize;
    const Uint32 num_vars= regTabPtr->m_attributes[ind].m_no_of_varsize;
    const Uint32 num_dyns= regTabPtr->m_attributes[ind].m_no_of_dynamic;

    if (ind == DD)
    {
      jamDebug();
      Uint32 disk_fix_header_size =
        regTabPtr->m_offsets[DD].m_fix_header_size;
      req_struct->m_disk_ptr= (Tuple_header*)ptr;
      ptr += disk_fix_header_size;
    }
    order += num_fix;
    jamDebug();
    jamDataDebug(num_fix);
    if (num_vars || num_dyns)
    {
      jam();
      /* Init Varpart_copy struct */
      Varpart_copy * cp = (Varpart_copy*)ptr;
      cp->m_len = 0;
      ptr += Varpart_copy::SZ32;

      /* Prepare empty varsize part. */
      KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];

      if (num_vars)
      {
        jamDebug();
        dst->m_data_ptr= (char*)(((Uint16*)ptr)+num_vars+1);
        dst->m_offset_array_ptr= req_struct->var_pos_array[ind];
        dst->m_var_len_offset= num_vars;
        dst->m_max_var_offset= regTabPtr->m_offsets[ind].m_max_var_offset;

        Uint32 pos= 0;
        Uint16 *pos_ptr = req_struct->var_pos_array[ind];
        Uint16 *len_ptr = pos_ptr + num_vars;
        for (Uint32 i = 0; i < num_vars; i++)
        {
          * pos_ptr++ = pos;
          * len_ptr++ = pos;
          pos += AttributeDescriptor::getSizeInBytes(
              tab_descr[*order++]);
          jamDebug();
          jamDataDebug(pos);
        }

        // Disk/dynamic part is 32-bit aligned
        ptr = ALIGN_WORD(dst->m_data_ptr+pos);
        ndbassert(ptr == ALIGN_WORD(dst->m_data_ptr + 
              regTabPtr->m_offsets[ind].m_max_var_offset));
      }

      if (num_dyns)
      {
        const Uint32 num_dynvar= regTabPtr->m_attributes[ind].m_no_of_dyn_var;
        const Uint32 num_dynfix= regTabPtr->m_attributes[ind].m_no_of_dyn_fix;
        jam();
        /* Prepare empty dynamic part. */
        dst->m_dyn_data_ptr= (char *)ptr;
        dst->m_dyn_offset_arr_ptr= req_struct->var_pos_array[ind]+2*num_vars;
        dst->m_dyn_len_offset= num_dynvar+num_dynfix;
        dst->m_max_dyn_offset= regTabPtr->m_offsets[ind].m_max_dyn_offset;

        ptr = expand_dyn_part(dst,
            0,
            0,
            (Uint32*)tab_descr,
            order,
            num_dynvar,
            num_dynfix,
            regTabPtr->m_offsets[ind].m_dyn_null_words);
        order += (num_dynvar + num_dynfix);
      }

      ndbassert((UintPtr(ptr)&3) == 0);
    }
  }
  /**
   * The copy tuple will be copied directly into the rowid position of
   * the tuple. Since we use the GCI in this position to see if a row
   * has changed we need to ensure that the GCI value is initialised,
   * otherwise we will not count inserts as a changed row.
   */
  *req_struct->m_tuple_ptr->get_mm_gci(regTabPtr) = 0;

  // Set all null bits
  std::memset(req_struct->m_tuple_ptr->m_null_bits+
      regTabPtr->m_offsets[MM].m_null_offset, 0xFF,
      4*regTabPtr->m_offsets[MM].m_null_words);
  std::memset(req_struct->m_disk_ptr->m_null_bits+
      regTabPtr->m_offsets[DD].m_null_offset, 0xFF,
      4*regTabPtr->m_offsets[DD].m_null_words);
}

int Dbtup::handleInsertReq(Signal* signal,
    Ptr<Operationrec> regOperPtr,
    FragrecordPtr fragPtr,
    Tablerec* regTabPtr,
    KeyReqStruct *req_struct,
    Local_key ** accminupdateptr,
    bool is_refresh)
{
  Uint32 tup_version = 1;
  Fragrecord* regFragPtr = fragPtr.p;
  Uint32 *ptr = nullptr;
  Tuple_header *dst;
  Tuple_header *base = req_struct->m_tuple_ptr;
  Tuple_header *org = base;
  Tuple_header *tuple_ptr;

  bool disk = regTabPtr->m_no_of_disk_attributes > 0 && !is_refresh;
  bool mem_insert = regOperPtr.p->is_first_operation();
  bool disk_insert = mem_insert && disk;
  bool vardynsize = (regTabPtr->m_attributes[MM].m_no_of_varsize ||
      regTabPtr->m_attributes[MM].m_no_of_dynamic);
  bool varalloc = vardynsize || regTabPtr->m_bits & Tablerec::TR_ForceVarPart;
  bool rowid = req_struct->m_use_rowid;
  bool update_acc = false; 
  Uint32 real_page_id = regOperPtr.p->m_tuple_location.m_page_no;
  Uint32 frag_page_id = req_struct->frag_page_id;

  union {
    Uint32 sizes[4];
    Uint64 cmp[2];
  };
  cmp[0] = cmp[1] = 0;

  if (ERROR_INSERTED(4014))
  {
    dst = 0;
    goto trans_mem_error;
  }

  dst = alloc_copy_tuple(regTabPtr, &regOperPtr.p->m_copy_tuple_location);

  if (unlikely(dst == nullptr))
  {
    goto trans_mem_error;
  }
  tuple_ptr= req_struct->m_tuple_ptr = dst;
  set_change_mask_info(regTabPtr, get_change_mask_ptr(regTabPtr, dst));

  if (mem_insert)
  {
    jamDebug();
    prepare_initial_insert(req_struct, regOperPtr.p, regTabPtr, is_refresh);
  }
  else
  {
    Operationrec* prevOp= req_struct->prevOpPtr.p;
    ndbassert(prevOp->op_type == ZDELETE);
    tup_version= prevOp->op_struct.bit_field.tupVersion + 1;

    if(unlikely(!prevOp->is_first_operation()))
    {
      jam();
      org= get_copy_tuple(&prevOp->m_copy_tuple_location);
    }
    else
    {
      jamDebug();
    }
    if (regTabPtr->need_expand())
    {
      jamDebug();
      expand_tuple(req_struct, sizes, org, regTabPtr, !disk_insert);
      std::memset(req_struct->m_disk_ptr->m_null_bits+
          regTabPtr->m_offsets[DD].m_null_offset, 0xFF,
          4*regTabPtr->m_offsets[DD].m_null_words);

      Uint32 bm_size_in_bytes= 4*(regTabPtr->m_offsets[MM].m_dyn_null_words);
      if (bm_size_in_bytes)
      {
        Uint32* ptr = 
          (Uint32*)req_struct->m_var_data[MM].m_dyn_data_ptr;
        std::memset(ptr, 0, bm_size_in_bytes);
        * ptr = bm_size_in_bytes >> 2;
      }
    } 
    else
    {
      jamDebug();
      memcpy(dst, org, 4*regTabPtr->m_offsets[MM].m_fix_header_size);
      tuple_ptr->m_header_bits |= Tuple_header::COPY_TUPLE;
    }
    std::memset(tuple_ptr->m_null_bits+
        regTabPtr->m_offsets[MM].m_null_offset, 0xFF,
        4*regTabPtr->m_offsets[MM].m_null_words);
  }

  int res;
  if (disk_insert)
  {
    jamDebug();
    if (ERROR_INSERTED(4015))
    {
      terrorCode = 1501;
      goto log_space_error;
    }

    {
      D("Logfile_client - handleInsertReq");
      Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
      DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
            instance(),
            __LINE__,
            regOperPtr.p->m_undo_buffer_space));
      res= lgman.alloc_log_space(regOperPtr.p->m_undo_buffer_space,
          true,
          !req_struct->m_nr_copy_or_redo,
          jamBuffer());
      jamDebug();
      jamDataDebug(regOperPtr.p->m_undo_buffer_space);
    }
    if (unlikely(res))
    {
      jam();
      terrorCode= res;
      goto log_space_error;
    }
  }

  regOperPtr.p->op_struct.bit_field.tupVersion=
    tup_version & ZTUP_VERSION_MASK;
  tuple_ptr->set_tuple_version(tup_version);

  if (ERROR_INSERTED(4016))
  {
    terrorCode = ZAI_INCONSISTENCY_ERROR;
    goto update_error;
  }

  if (regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits)
  {
    jamDebug();
    Uint32 attrId =
      regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

    store_extra_row_bits(attrId, regTabPtr, tuple_ptr, /* default */ 0, false);
  }

  if (!(is_refresh ||
        regTabPtr->m_default_value_location.isNull()))
  {
    jamDebug();
    Uint32 default_values_len;
    /* Get default values ptr + len for this table */
    Uint32* default_values = get_default_ptr(regTabPtr, default_values_len);
    ndbrequire(default_values_len != 0 && default_values != NULL);
    /*
     * Update default values into row first,
     * next update with data received from the client.
     */
    if(unlikely((res = updateAttributes(req_struct, default_values,
              default_values_len)) < 0))
    {
      jam();
      terrorCode = Uint32(-res);
      goto update_error;
    }
  }

  if (unlikely(req_struct->interpreted_exec))
  {
    jam();

    /* Interpreted insert only processes the finalUpdate section */
    const Uint32 RinitReadLen= cinBuffer[0];
    const Uint32 RexecRegionLen= cinBuffer[1];
    const Uint32 RfinalUpdateLen= cinBuffer[2];
    //const Uint32 RfinalRLen= cinBuffer[3];
    //const Uint32 RsubLen= cinBuffer[4];

    const Uint32 offset = 5 + RinitReadLen + RexecRegionLen;
    req_struct->log_size = 0;

    if (unlikely((res = updateAttributes(req_struct, &cinBuffer[offset],
              RfinalUpdateLen)) < 0))
    {
      jam();
      terrorCode = Uint32(-res);
      goto update_error;
    }

    /**
     * Send normal format AttrInfo back to LQH for
     * propagation
     */
    req_struct->log_size = RfinalUpdateLen;
    MEMCOPY_NO_WORDS(&clogMemBuffer[0],
        &cinBuffer[offset],
        RfinalUpdateLen);

    if (unlikely(sendLogAttrinfo(signal,
            req_struct,
            RfinalUpdateLen,
            regOperPtr.p) != 0))
    {
      jam();
      goto update_error;
    }
  }
  else
  {
    /* Normal insert */
    if (unlikely((res = updateAttributes(req_struct, &cinBuffer[0],
              req_struct->attrinfo_len)) < 0))
    {
      jam();
      terrorCode = Uint32(-res);
      goto update_error;
    }
  }

  if (ERROR_INSERTED(4017))
  {
    goto null_check_error;
  }
  if (unlikely(checkNullAttributes(req_struct,
          regTabPtr,
          is_refresh) == false))
  {
    goto null_check_error;
  }

  if (req_struct->m_is_lcp)
  {
    jam();
    sizes[2+MM] = req_struct->m_lcp_varpart_len;
  }
  else if (regTabPtr->need_shrink())
  {
    jamDebug();
    shrink_tuple(req_struct, sizes+2, regTabPtr, true);
  }

  if (ERROR_INSERTED(4025))
  {
    goto mem_error;
  }

  if (ERROR_INSERTED(4026))
  {
    CLEAR_ERROR_INSERT_VALUE;
    goto mem_error;
  }

  if (ERROR_INSERTED(4027) && (rand() % 100) > 25)
  {
    goto mem_error;
  }

  if (ERROR_INSERTED(4028) && (rand() % 100) > 25)
  {
    CLEAR_ERROR_INSERT_VALUE;
    goto mem_error;
  }

  /**
   * Alloc memory
   */
  if(mem_insert)
  {
    terrorCode = 0;
    if (!rowid)
    {
      if (ERROR_INSERTED(4018))
      {
        goto mem_error;
      }

      if (!varalloc)
      {
        jam();
        ptr= alloc_fix_rec(jamBuffer(),
            &terrorCode,
            regFragPtr,
            regTabPtr,
            &regOperPtr.p->m_tuple_location,
            &frag_page_id);
      } 
      else 
      {
        jam();
        regOperPtr.p->m_tuple_location.m_file_no= sizes[2+MM];
        ptr= alloc_var_row(&terrorCode,
            regFragPtr, regTabPtr,
            sizes[2+MM],
            &regOperPtr.p->m_tuple_location,
            &frag_page_id,
            false);
      }
      if (unlikely(ptr == 0))
      {
        goto mem_error;
      }
      req_struct->m_use_rowid = true;
    }
    else
    {
      regOperPtr.p->m_tuple_location = req_struct->m_row_id;
      if (ERROR_INSERTED(4019))
      {
        terrorCode = ZROWID_ALLOCATED;
        goto alloc_rowid_error;
      }

      if (!varalloc)
      {
        jam();
        ptr= alloc_fix_rowid(&terrorCode,
            regFragPtr,
            regTabPtr,
            &regOperPtr.p->m_tuple_location,
            &frag_page_id);
      } 
      else 
      {
        jam();
        regOperPtr.p->m_tuple_location.m_file_no= sizes[2+MM];
        ptr= alloc_var_row(&terrorCode,
            regFragPtr, regTabPtr,
            sizes[2+MM],
            &regOperPtr.p->m_tuple_location,
            &frag_page_id,
            true);
      }
      if (unlikely(ptr == 0))
      {
        jam();
        goto alloc_rowid_error;
      }
    }
    /**
     * Arriving here we have acquired the fragment page mutex in
     * either alloc_fix_rec (can be called from alloc_var_rec) or
     * alloc_fix_rowid (can be called from alloc_var_rowid).
     *
     * Thus as soon as we release the fragment mutex the row will
     * be visible to the TUP scan.
     */
    regOperPtr.p->fragPageId = frag_page_id;
    real_page_id = regOperPtr.p->m_tuple_location.m_page_no;
    update_acc = true; /* Will be updated later once success is known */

    base = (Tuple_header*)ptr;
    regOperPtr.p->op_struct.bit_field.in_active_list = true;
    base->m_operation_ptr_i= regOperPtr.i;
    ndbassert(!m_is_in_query_thread);

#ifdef DEBUG_DELETE
    char *insert_str;
    if (req_struct->m_is_lcp)
    {
      insert_str = (char*)"LCP_INSERT";
    }
    else
    {
      insert_str = (char*)"INSERT";
    }
    DEB_DELETE(("(%u)%s: tab(%u,%u) row(%u,%u)",
          instance(),
          insert_str,
          regFragPtr->fragTableId,
          regFragPtr->fragmentId,
          frag_page_id,
          regOperPtr.p->m_tuple_location.m_page_idx));
#endif

    /**
     * The LCP_SKIP and LCP_DELETE flags must be retained even when allocating
     * a new row since they record state for the rowid and not for the record
     * as such. So we need to know state of rowid in LCP scans.
     */
    Uint32 old_header_keep =
      base->m_header_bits &
      (Tuple_header::LCP_SKIP | Tuple_header::LCP_DELETE);
    base->m_header_bits= Tuple_header::ALLOC |
      (sizes[2+MM] > 0 ? Tuple_header::VAR_PART : 0) |
      old_header_keep;
    if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
    {
      jam();
      if (disk_insert)
      {
        jam();
        /**
         * If mem_insert is true and disk_insert isn't true, this means
         * that this a Refresh operation, in this case we will not create
         * any disk part. Thus we can avoid setting DISK_VAR_PART to
         * ensure we don't attempt to retrieve the disk page.
         */
        base->m_header_bits |= Tuple_header::DISK_VAR_PART;
      }
    }
    if (disk_insert)
    {
      Local_key tmp;
      Uint32 size =
        ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0) ?
        1 : (sizes[2+DD] + 1);

      jamDebug();
      jamDataDebug(size);

      if (ERROR_INSERTED(4021))
      {
        terrorCode = 1601;
        goto disk_prealloc_error;
      }


      int ret= disk_page_prealloc(signal,
          fragPtr,
          regTabPtr,
          &tmp,
          size);
      if (unlikely(ret < 0))
      {
        jam();
        terrorCode = -ret;
        goto disk_prealloc_error;
      }

      jamDebug();
      jamDataDebug(tmp.m_file_no);
      jamDataDebug(tmp.m_page_no);
      regOperPtr.p->op_struct.bit_field.m_disk_preallocated= 1;
      tmp.m_page_idx= size;
      /**
       * We update the disk row reference both in the allocated row and
       * in the allocated copy row. The allocated row reference is used
       * in load_diskpage if we do any further operations on the
       * row in the same transaction.
       */
      memcpy(tuple_ptr->get_disk_ref_ptr(regTabPtr), &tmp, sizeof(tmp));
      memcpy(base->get_disk_ref_ptr(regTabPtr), &tmp, sizeof(tmp));

      /**
       * Set ref from disk to mm
       */
      Local_key ref = regOperPtr.p->m_tuple_location;
      ref.m_page_no = frag_page_id;

      ndbrequire(ref.m_page_idx < Tup_page::DATA_WORDS);
      Tuple_header* disk_ptr= req_struct->m_disk_ptr;
      DEB_DISK(("(%u) set_base_record(%u,%u).%u on row(%u,%u)",
            instance(),
            tmp.m_file_no,
            tmp.m_page_no,
            tmp.m_page_idx,
            ref.m_page_no,
            ref.m_page_idx));

      disk_ptr->set_base_record_ref(ref);
    }
    setChecksum(req_struct->m_tuple_ptr, regTabPtr);
    /**
     * At this point we hold the fragment mutex to ensure that TUP scans
     * don't see the rows until the row is ready for reading.
     *
     * After releasing the mutex here the row becomes visible to TUP scans
     * and thus checksum needs to be correct on the copy row, the checksum
     * on the row itself isn't checked before reading or updating unless it
     * is used for reading. So no need to update it already here. It will
     * be set when we commit the change.
     */
    release_frag_mutex(regFragPtr, frag_page_id, jamBuffer());
  }
  else 
  {
    if (ERROR_INSERTED(4020))
    {
      c_lqh->upgrade_to_exclusive_frag_access();
      goto size_change_error;
    }

    if (regTabPtr->need_shrink() && cmp[0] != cmp[1] &&
        unlikely(handle_size_change_after_update(signal,
            req_struct,
            base,
            regOperPtr.p,
            regFragPtr,
            regTabPtr,
            sizes) != 0))
    {
      goto size_change_error;
    }
    req_struct->m_use_rowid = false;
    /**
     * The main row header bits are updated through a local variable
     * such that we can do the change under mutex protection after
     * finalizing the writes on the row.
     */
    m_base_header_bits &= ~(Uint32)Tuple_header::FREE;
  }

  if (req_struct->m_reorg != ScanFragReq::REORG_ALL)
  {
    handle_reorg(req_struct, regFragPtr->fragStatus);
  }

  /* Have been successful with disk + mem, update ACC to point to
   * new record if necessary
   * Failures in disk alloc will skip this part
   */
  if (update_acc)
  {
    /* Acc stores the local key with the frag_page_id rather
     * than the real_page_id
     */
    jamDebug();
    ndbassert(regOperPtr.p->m_tuple_location.m_page_no == real_page_id);

    Local_key accKey = regOperPtr.p->m_tuple_location;
    accKey.m_page_no = frag_page_id;
    ** accminupdateptr = accKey;
  }
  else
  {
    * accminupdateptr = 0; // No accminupdate should be performed
  }
  if (!mem_insert)
  {
    /**
     * No need to protect this checksum write, we only perform it here for
     * non-first inserts since first insert operations are handled above
     * while holding the mutex. For non-first operations the row is not
     * visible to others at this time, copy rows are not even visible to
     * TUP scans, thus no need to protect it here. The row becomes visible
     * when inserted into the active list after returning from this call.
     */
    jamDebug();
    setChecksum(req_struct->m_tuple_ptr, regTabPtr);
  }
  set_tuple_state(regOperPtr.p, TUPLE_PREPARED);
  return 0;

size_change_error:
  jam();
  terrorCode = ZMEM_NOMEM_ERROR;
  goto exit_error;

trans_mem_error:
  jam();
  terrorCode= ZNO_COPY_TUPLE_MEMORY_ERROR;
  regOperPtr.p->m_undo_buffer_space = 0;
  if (mem_insert)
    regOperPtr.p->m_tuple_location.setNull();
  regOperPtr.p->m_copy_tuple_location.setNull();
  tupkeyErrorLab(req_struct);
  return -1;

null_check_error:
  jam();
  terrorCode= ZNO_ILLEGAL_NULL_ATTR;
  goto update_error;

mem_error:
  jam();
  if (terrorCode == 0)
  {
    terrorCode= ZMEM_NOMEM_ERROR;
  }
  goto update_error;

log_space_error:
  jam();
  regOperPtr.p->m_undo_buffer_space = 0;
alloc_rowid_error:
  jam();
update_error:
  jam();
  if (mem_insert)
  {
    regOperPtr.p->m_tuple_location.setNull();
  }
exit_error:
  if (!regOperPtr.p->m_tuple_location.isNull())
  {
    jam();
    /* Memory allocated, abort insert, releasing memory if appropriate */
    signal->theData[0] = regOperPtr.i;
    do_tup_abortreq(signal, ZSKIP_TUX_TRIGGERS | ZABORT_DEALLOC);
  }
  tupkeyErrorLab(req_struct);
  return -1;

disk_prealloc_error:
  jam();
  base->m_header_bits |= Tuple_header::FREE;
  setInvalidChecksum(base, regTabPtr);
  release_frag_mutex(regFragPtr, frag_page_id, jamBuffer());
  c_lqh->upgrade_to_exclusive_frag_access_no_return();
  goto exit_error;
}

/* ---------------------------------------------------------------- */
/* ---------------------------- DELETE ---------------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::handleDeleteReq(Signal* signal,
                           Operationrec* regOperPtr,
                           Fragrecord* regFragPtr,
                           Tablerec* regTabPtr,
                           KeyReqStruct *req_struct,
                           bool disk)
{
  Uint32 copy_bits = 0;
  Tuple_header* dst = alloc_copy_tuple(regTabPtr,
      &regOperPtr->m_copy_tuple_location);
  if (unlikely(dst == 0))
  {
    jam();
    terrorCode = ZNO_COPY_TUPLE_MEMORY_ERROR;
    goto error;
  }

  // delete must set but not increment tupVersion
  if (unlikely(!regOperPtr->is_first_operation()))
  {
    jam();
    Operationrec* prevOp= req_struct->prevOpPtr.p;
    regOperPtr->op_struct.bit_field.tupVersion=
      prevOp->op_struct.bit_field.tupVersion;
    // make copy since previous op is committed before this one
    const Tuple_header* org = get_copy_tuple(&prevOp->m_copy_tuple_location);
    Uint32 len = regTabPtr->total_rec_size -
      Uint32(((Uint32*)dst) -
          get_copy_tuple_raw(&regOperPtr->m_copy_tuple_location));
    memcpy(dst, org, 4 * len);
    req_struct->m_tuple_ptr = dst;
    copy_bits = org->m_header_bits;
    if (regTabPtr->m_no_of_disk_attributes)
    {
      ndbrequire(disk);
      memcpy(dst->get_disk_ref_ptr(regTabPtr),
          req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
          sizeof(Local_key));
    }
  }
  else
  {
    regOperPtr->op_struct.bit_field.tupVersion=
      req_struct->m_tuple_ptr->get_tuple_version();
    dst->m_header_bits = req_struct->m_tuple_ptr->m_header_bits;
    copy_bits = dst->m_header_bits;
    if (regTabPtr->m_no_of_disk_attributes)
    {
      ndbrequire(disk);
      memcpy(dst->get_disk_ref_ptr(regTabPtr),
          req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
          sizeof(Local_key));
    }
  }
  req_struct->changeMask.set();
  set_change_mask_info(regTabPtr, get_change_mask_ptr(regTabPtr, dst));

  if (disk && regOperPtr->m_undo_buffer_space == 0)
  {
    jam();
    ndbrequire(!(copy_bits & Tuple_header::DISK_ALLOC));
    regOperPtr->op_struct.bit_field.m_wait_log_buffer = 1;
    regOperPtr->op_struct.bit_field.m_load_diskpage_on_commit = 1;
    /**
     * Arriving here we cannot have the flag DISK_ALLOC set since
     * this would require m_undo_buffer_space to be set > 0.
     *
     * The length of the disk part is retrieved in get_dd_info,
     * this method retrieves a few other things that we are not
     * interested in here, so use dummy variables for those.
     *
     * Even if we do multiple insert-delete pairs and even updates
     * in between if this DELETE becomes the final delete, it will
     * always use the UNDO information from the stored row, thus
     * even for multi-row operations we retrieve the size of the
     * UNDO log information from the stored row.
     *
     * We calculate the space to write into the UNDO log here. We
     * allocate space in the UNDO log files here to ensure that
     * there is space for the UNDO log in the files. At commit time
     * we need to allocate space in the log buffer before actually
     * writing the UNDO log. The actual write to the UNDO log happens
     * as a background task that writes from the UNDO log buffer.
     */
    Uint32 undo_len;
    if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
    {
      jamDebug();
      undo_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
        (regTabPtr->m_offsets[DD].m_fix_header_size - 1);
    }
    else
    {
      jamDebug();
      Local_key key;
      const Uint32 *disk_ref = dst->get_disk_ref_ptr(regTabPtr);
      memcpy(&key, disk_ref, sizeof(key));
      key.m_page_no= req_struct->m_disk_page_ptr.i;
      ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
      Uint32 disk_len = 0;
      Uint32 *src_ptr = get_dd_info(&req_struct->m_disk_page_ptr,
          key,
          regTabPtr,
          disk_len);
      (void)src_ptr;
      undo_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
        (disk_len - 1);
    }
    regOperPtr->m_undo_buffer_space = undo_len;
    jamDebug();
    jamDataDebug(regOperPtr->m_undo_buffer_space);
    {
      D("Logfile_client - handleDeleteReq");
      Logfile_client lgman(this, c_lgman, regFragPtr->m_logfile_group_id);
      DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
            instance(),
            __LINE__,
            regOperPtr->m_undo_buffer_space));
      terrorCode= lgman.alloc_log_space(regOperPtr->m_undo_buffer_space,
          true,
          !req_struct->m_nr_copy_or_redo,
          jamBuffer());
    }
    jamDataDebug(regOperPtr->m_undo_buffer_space);
    if (unlikely(terrorCode))
    {
      jam();
      regOperPtr->m_undo_buffer_space= 0;
      goto error;
    }
  }

  set_tuple_state(regOperPtr, TUPLE_PREPARED);

  if (req_struct->attrinfo_len == 0)
  {
    return 0;
  }

  if (regTabPtr->need_expand(disk))
  {
    prepare_read(req_struct, regTabPtr, disk);
  }

  {
    /* Delete happens in LDM thread, so no need to protect it */
    if (unlikely(((regTabPtr->m_bits & Tablerec::TR_Checksum) &&
            (calculateChecksum(req_struct->m_tuple_ptr, regTabPtr) != 0)) ||
          ERROR_INSERTED(4036)))
    {
      jam();
      return corruptedTupleDetected(req_struct, regTabPtr);
    }
    Uint32 RlogSize;
    int ret= handleReadReq(signal, regOperPtr, regTabPtr, req_struct);
    if (ret == 0 && (RlogSize= req_struct->log_size))
    {
      jam();
      sendLogAttrinfo(signal, req_struct, RlogSize, regOperPtr);
    }
    return ret;
  }

error:
  tupkeyErrorLab(req_struct);
  return -1;
}

int
Dbtup::handleRefreshReq(Signal* signal,
                        Ptr<Operationrec> regOperPtr,
                        FragrecordPtr regFragPtr,
                        Tablerec* regTabPtr,
                        KeyReqStruct *req_struct,
                        bool disk)
{
  /* Here we setup the tuple so that a transition to its current
   * state can be observed by SUMA's detached triggers.
   *
   * If the tuple does not exist then we fabricate a tuple
   * so that it can appear to be 'deleted'.
   *   The fabricated tuple may have invalid NULL values etc.
   * If the tuple does exist then we fabricate a null-change
   * update to the tuple.
   *
   * The logic differs depending on whether there are already
   * other operations on the tuple in this transaction.
   * No other operations (including Refresh) are allowed after
   * a refresh.
   */
  Uint32 refresh_case;
  if (likely(regOperPtr.p->is_first_operation()))
  {
    jam();
    if (Local_key::isInvalid(req_struct->frag_page_id,
          regOperPtr.p->m_tuple_location.m_page_idx))
    {
      jam();
      refresh_case = Operationrec::RF_SINGLE_NOT_EXIST;
      /**
       * This is refresh of non-existing tuple...
       *   i.e "delete", reuse initial insert
       */
      Local_key accminupdate;
      Local_key *accminupdateptr = &accminupdate;

      /**
       * We don't need ...in this scenario
       * - disk
       * - default values
       *
       * We signal this to handleInsertReq with is_refresh flag
       * set to true.
       */
      regOperPtr.p->op_type = ZINSERT;

      int res = handleInsertReq(signal, regOperPtr, regFragPtr, regTabPtr,
          req_struct, &accminupdateptr, true);

      if (unlikely(res == -1)) {
        jam();
        return -1;
      }

      regOperPtr.p->op_type = ZREFRESH;

      if (accminupdateptr)
      {
        /**
         * Update ACC local-key, once *everything* has completed successfully
         */
        jamDebug();
        c_lqh->accminupdate(signal,
            regOperPtr.p->userpointer,
            accminupdateptr);
      }
    }
    else
    {
      refresh_case = Operationrec::RF_SINGLE_EXIST;
      // g_eventLogger->info("case 2");
      jam();

      Tuple_header* origTuple = req_struct->m_tuple_ptr;
      Uint32 tup_version_save = origTuple->get_tuple_version();
      {
        /* Set new row version and update the tuple header */
        Uint32 old_header = origTuple->m_header_bits;
        Uint32 new_tup_version = decr_tup_version(tup_version_save);
        origTuple->set_tuple_version(new_tup_version);
        Uint32 new_header = origTuple->m_header_bits;
        updateChecksum(origTuple, regTabPtr, old_header, new_header);
      }
      m_base_header_bits = origTuple->m_header_bits;
      int res = handleUpdateReq(signal, regOperPtr.p, regFragPtr.p,
          regTabPtr, req_struct, disk);

      /* Now we must reset the original tuple header back
       * to the original version.
       * The copy tuple will have the correct version due to
       * the update incrementing it.
       * On commit, the tuple becomes the copy tuple.
       * On abort, the original tuple remains.  If we don't
       * reset it here, then aborts cause the version to
       * decrease
       *
       * We also need to recalculate checksum since we're changing the
       * row here.
       */
      {
        origTuple->m_header_bits = m_base_header_bits;
        Uint32 old_header = m_base_header_bits;
        origTuple->set_tuple_version(tup_version_save);
        Uint32 new_header = origTuple->m_header_bits;
        updateChecksum(origTuple, regTabPtr, old_header, new_header);
        m_base_header_bits = origTuple->m_header_bits;
      }
      if (unlikely(res == -1))
      {
        jam();
        return -1;
      }
    }
  }
  else
  {
    /* Not first operation on tuple in transaction */
    jam();

    Uint32 tup_version_save =
      req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion;
    Uint32 new_tup_version = decr_tup_version(tup_version_save);
    req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion = new_tup_version;

    int res;
    if (req_struct->prevOpPtr.p->op_type == ZDELETE)
    {
      refresh_case = Operationrec::RF_MULTI_NOT_EXIST;
      jam();
      /**
       * We don't need ...in this scenario
       * - default values
       *
       * We keep disk attributes to avoid issues with 'insert'
       * We signal this to handleInsertReq with is_refresh flag
       * set to true.
       */
      regOperPtr.p->op_type = ZINSERT;

      /**
       * This is multi-update + DELETE + REFRESH
       */
      Local_key * accminupdateptr = 0;
      res = handleInsertReq(signal,
          regOperPtr,
          regFragPtr,
          regTabPtr,
          req_struct,
          &accminupdateptr,
          true);
      if (unlikely(res == -1))
      {
        jam();
        return -1;
      }
      regOperPtr.p->op_type = ZREFRESH;
    }
    else
    {
      jam();
      refresh_case = Operationrec::RF_MULTI_EXIST;
      /**
       * This is multi-update + INSERT/UPDATE + REFRESH
       */
      res = handleUpdateReq(signal, regOperPtr.p, regFragPtr.p,
          regTabPtr, req_struct, disk);
    }
    req_struct->prevOpPtr.p->op_struct.bit_field.tupVersion = tup_version_save;
    if (unlikely(res == -1))
    {
      jam();
      return -1;
    }
  }

  /* Store the refresh scenario in the copy tuple location */
  // TODO : Verify this is never used as a copy tuple location!
  regOperPtr.p->m_copy_tuple_location.m_file_no = refresh_case;
  return 0;
}

bool
Dbtup::checkNullAttributes(KeyReqStruct * req_struct,
                           Tablerec* regTabPtr,
                           bool is_refresh)
{
  // Implement checking of updating all not null attributes in an insert here.
  Bitmask<MAXNROFATTRIBUTESINWORDS> attributeMask;  
  /* 
   * The idea here is maybe that changeMask is not-null attributes
   * and must contain notNullAttributeMask.  But:
   *
   * 1. changeMask has all bits set on insert
   * 2. not-null is checked in each UpdateFunction
   * 3. the code below does not work except trivially due to 1.
   *
   * XXX remove or fix
   */
  attributeMask.clear();
  attributeMask.bitOR(req_struct->changeMask);
  if (unlikely(is_refresh))
  {
    /**
     * Update notNullAttributeMask  to only include primary keys
     */
    Bitmask<MAXNROFATTRIBUTESINWORDS> tableMask;
    tableMask.clear();
    const Uint32 * primarykeys = regTabPtr->readKeyArray;
    for (Uint32 i = 0; i<regTabPtr->noOfKeyAttr; i++)
      tableMask.set(primarykeys[i] >> 16);
    attributeMask.bitAND(tableMask);
    attributeMask.bitXOR(tableMask);
  }
  else
  {
    attributeMask.bitAND(regTabPtr->notNullAttributeMask);
    attributeMask.bitXOR(regTabPtr->notNullAttributeMask);
  }
  if (!attributeMask.isclear())
  {
    return false;
  }
  return true;
}

/* ---------------------------------------------------------------- */
/* THIS IS THE START OF THE INTERPRETED EXECUTION OF UPDATES. WE    */
/* START BY LINKING ALL ATTRINFO'S IN A DOUBLY LINKED LIST (THEY ARE*/
/* ALREADY IN A LINKED LIST). WE ALLOCATE A REGISTER MEMORY (EQUAL  */
/* TO AN ATTRINFO RECORD). THE INTERPRETER GOES THROUGH FOUR  PHASES*/
/* DURING THE FIRST PHASE IT IS ONLY ALLOWED TO READ ATTRIBUTES THAT*/
/* ARE SENT TO THE CLIENT APPLICATION. DURING THE SECOND PHASE IT IS*/
/* ALLOWED TO READ FROM ATTRIBUTES INTO REGISTERS, TO UPDATE        */
/* ATTRIBUTES BASED ON EITHER A CONSTANT VALUE OR A REGISTER VALUE, */
/* A DIVERSE SET OF OPERATIONS ON REGISTERS ARE AVAILABLE AS WELL.  */
/* IT IS ALSO POSSIBLE TO PERFORM JUMPS WITHIN THE INSTRUCTIONS THAT*/
/* BELONGS TO THE SECOND PHASE. ALSO SUBROUTINES CAN BE CALLED IN   */
/* THIS PHASE. THE THIRD PHASE IS TO AGAIN READ ATTRIBUTES AND      */
/* FINALLY THE FOURTH PHASE READS SELECTED REGISTERS AND SEND THEM  */
/* TO THE CLIENT APPLICATION.                                       */
/* THERE IS A FIFTH REGION WHICH CONTAINS SUBROUTINES CALLABLE FROM */
/* THE INTERPRETER EXECUTION REGION.                                */
/* THE FIRST FIVE WORDS WILL GIVE THE LENGTH OF THE FIVE REGIONS    */
/*                                                                  */
/* THIS MEANS THAT FROM THE APPLICATIONS POINT OF VIEW THE DATABASE */
/* CAN HANDLE SUBROUTINE CALLS WHERE THE CODE IS SENT IN THE REQUEST*/
/* THE RETURN PARAMETERS ARE FIXED AND CAN EITHER BE GENERATED      */
/* BEFORE THE EXECUTION OF THE ROUTINE OR AFTER.                    */
/*                                                                  */
/* IN LATER VERSIONS WE WILL ADD MORE THINGS LIKE THE POSSIBILITY   */
/* TO ALLOCATE MEMORY AND USE THIS AS LOCAL STORAGE. IT IS ALSO     */
/* IMAGINABLE TO HAVE SPECIAL ROUTINES THAT CAN PERFORM CERTAIN     */
/* OPERATIONS ON BLOB'S DEPENDENT ON WHAT THE BLOB REPRESENTS.      */
/*                                                                  */
/*                                                                  */
/*       -----------------------------------------                  */
/*       +   INITIAL READ REGION                 +                  */
/*       -----------------------------------------                  */
/*       +   INTERPRETED EXECUTE  REGION         +                  */
/*       -----------------------------------------                  */
/*       +   FINAL UPDATE REGION                 +                  */
/*       -----------------------------------------                  */
/*       +   FINAL READ REGION                   +                  */
/*       -----------------------------------------                  */
/*       +   SUBROUTINE REGION (or parameter)    +                  */
/*       -----------------------------------------                  */
/*                                                                  */
/* For read operations it only makes sense to first perform the     */
/* interpreted execution (this will perform condition pushdown      */
/* where we evaluate the conditions that are not evaluated by       */
/* ranges implied by the scan operation. These conditions pushed    */
/* down can essentially check any type of condition.                */
/*                                                                  */
/* Since it only makes sense to interpret before reading we delay   */
/* the initial read to after interpreted execution for read         */
/* operations. This is safe from a protocol point of view since the */
/* interpreted execution cannot generate Attrinfo data.             */
/*                                                                  */
/* For updates it still makes sense to handle initial read and      */
/* final read separately since we might want to read values before  */
/* and after changes, the interpreter can write column values.      */
/* ---------------------------------------------------------------- */
/* ---------------------------------------------------------------- */
/* ----------------- INTERPRETED EXECUTION  ----------------------- */
/* ---------------------------------------------------------------- */
int Dbtup::interpreterStartLab(Signal* signal,
                               KeyReqStruct *req_struct)
{
  Operationrec * const regOperPtr = req_struct->operPtrP;
  int TnoDataRW;
  Uint32 RtotalLen, start_index, dstLen;
  Uint32 *dst;

  Uint32 RinitReadLen= cinBuffer[0];
  Uint32 RexecRegionLen= cinBuffer[1];
  Uint32 RfinalUpdateLen= cinBuffer[2];
  Uint32 RfinalRLen= cinBuffer[3];
  Uint32 RsubLen= cinBuffer[4];

  jamDebug();

  Uint32 RattrinbufLen= req_struct->attrinfo_len;
  const BlockReference sendBref= req_struct->rec_blockref;

  const Uint32 node = refToNode(sendBref);
  if(node != 0 && node != getOwnNodeId())
  {
    start_index= 25;
  }
  else
  {
    jamDebug();
    /**
     * execute direct
     */
    start_index= TransIdAI::HeaderLength;  //3;
  }
  dst= &signal->theData[start_index];
  dstLen= (MAX_READ / 4) - start_index;

  RtotalLen= RinitReadLen;
  RtotalLen += RexecRegionLen;
  RtotalLen += RfinalUpdateLen;
  RtotalLen += RfinalRLen;
  RtotalLen += RsubLen;

  Uint32 RattroutCounter= 0;
  Uint32 RinstructionCounter= 5;

  /* All information to be logged/propagated to replicas
   * is generated from here on so reset the log word count
   *
   * Note that in case attrInfo contain multiple params for the
   * interpreterCode, we will only copy one of them into the cinBuffer[].
   * Thus, 'RtotalLen + 5' may be '<' than RattrinbufLen.
   */
  Uint32 RlogSize= req_struct->log_size= 0;
  if (likely(((RtotalLen + 5) <= RattrinbufLen) &&
        (RattrinbufLen >= 5) &&
        (RtotalLen + 5 < ZATTR_BUFFER_SIZE)))
  {
    /* ---------------------------------------------------------------- */
    // We start by checking consistency. We must have the first five
    // words of the ATTRINFO to give us the length of the regions. The
    // size of these regions must be the same as the total ATTRINFO
    // length and finally the total length must be within the limits.
    /* ---------------------------------------------------------------- */

    if (likely(RinitReadLen > 0))
    {
      if (likely(regOperPtr->op_type == ZREAD))
      {
        jamDebug();
        RinstructionCounter += RinitReadLen;
      }
      else
      {
        jamDebug();
        /* ---------------------------------------------------------------- */
        // The first step that can be taken in the interpreter is to read
        // data of the tuple before any updates have been applied.
        /* ---------------------------------------------------------------- */
        TnoDataRW= readAttributes(req_struct,
                                  &cinBuffer[5],
                                  RinitReadLen,
                                  &dst[0],
                                  dstLen);
        if (TnoDataRW >= 0)
        {
          jamDebug();
          RattroutCounter= TnoDataRW;
          RinstructionCounter += RinitReadLen;
          RinitReadLen = 0;
        }
        else
        {
          jam();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
      }
    }
    if (RexecRegionLen > 0)
    {
      jamDebug();
      /* ---------------------------------------------------------------- */
      // The next step is the actual interpreted execution. This executes
      // a register-based virtual machine which can read and write attributes
      // to and from registers.
      /* ---------------------------------------------------------------- */
      Uint32 RsubPC= RinstructionCounter + RexecRegionLen 
        + RfinalUpdateLen + RfinalRLen;     
      TnoDataRW= interpreterNextLab(signal,
          req_struct,
          &clogMemBuffer[0],
          &cinBuffer[RinstructionCounter],
          RexecRegionLen,
          &cinBuffer[RsubPC],
          RsubLen,
          &coutBuffer[0],
          sizeof(coutBuffer) / 4);
      if (TnoDataRW != -1)
      {
        jamDebug();
        RinstructionCounter += RexecRegionLen;
        RlogSize= TnoDataRW;
      }
      else
      {
        jamDebug();
        /**
         * TUPKEY REF is sent from within interpreter
         */
        return -1;
      }
    }

    if ((RlogSize > 0) ||
        (RfinalUpdateLen > 0))
    {
      jamDebug();
      /* Operation updates row,
       * reset author pseudo-col before update takes effect
       * This should probably occur only if the interpreted program
       * did not explicitly write the value, but that requires a bit
       * to record whether the value has been written.
       */
      Tablerec* regTabPtr = req_struct->tablePtrP;
      Tuple_header* dst = req_struct->m_tuple_ptr;

      if (unlikely(regTabPtr->m_bits & Tablerec::TR_ExtraRowAuthorBits))
      {
        Uint32 attrId =
          regTabPtr->getExtraAttrId<Tablerec::TR_ExtraRowAuthorBits>();

        store_extra_row_bits(attrId, regTabPtr, dst, /* default */ 0, false);
      }
    }

    if (unlikely(RfinalUpdateLen > 0))
    {
      /* ---------------------------------------------------------------- */
      // We can also apply a set of updates without any conditions as part
      // of the interpreted execution.
      /* ---------------------------------------------------------------- */
      if (regOperPtr->op_type == ZUPDATE)
      {
        jamDebug();
        TnoDataRW= updateAttributes(req_struct,
            &cinBuffer[RinstructionCounter],
            RfinalUpdateLen);
        if (TnoDataRW >= 0)
        {
          jamDebug();
          MEMCOPY_NO_WORDS(&clogMemBuffer[RlogSize],
              &cinBuffer[RinstructionCounter],
              RfinalUpdateLen);
          RinstructionCounter += RfinalUpdateLen;
          RlogSize += RfinalUpdateLen;
        }
        else
        {
          jam();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
      }
      else
      {
        jamDebug();
        return TUPKEY_abort(req_struct, ZTRY_TO_UPDATE_ERROR);
      }
    }
    if (likely(RinitReadLen > 0))
    {
      jamDebug();
      TnoDataRW= readAttributes(req_struct,
                                &cinBuffer[5],
                                RinitReadLen,
                                &dst[0],
                                dstLen);
      if (TnoDataRW >= 0)
      {
        jamDebug();
        RattroutCounter = TnoDataRW;
      }
      else
      {
        jam();
        terrorCode = Uint32(-TnoDataRW);
        tupkeyErrorLab(req_struct);
        return -1;
      }
    }
    if (RfinalRLen > 0)
    {
      jamDebug();
      /* ---------------------------------------------------------------- */
      // The final action is that we can also read the tuple after it has
      // been updated.
      /* ---------------------------------------------------------------- */
      TnoDataRW= readAttributes(req_struct,
                                &cinBuffer[RinstructionCounter],
                                RfinalRLen,
                                &dst[RattroutCounter],
                                (dstLen - RattroutCounter));
      if (TnoDataRW >= 0)
      {
        jamDebug();
        RattroutCounter += TnoDataRW;
      }
      else
      {
        jam();
        terrorCode = Uint32(-TnoDataRW);
        tupkeyErrorLab(req_struct);
        return -1;
      }
    }
    /* Add log words explicitly generated here to existing log size
     *  - readAttributes can generate log for ANYVALUE column
     *    It adds the words directly to req_struct->log_size
     *    This is used for ANYVALUE and interpreted delete.
     */
    req_struct->log_size+= RlogSize;
    sendReadAttrinfo(signal, req_struct, RattroutCounter);
    if (RlogSize > 0)
    {
      return sendLogAttrinfo(signal, req_struct, RlogSize, regOperPtr);
    }
    return 0;
  }
  else
  {
    return TUPKEY_abort(req_struct, ZTOTAL_LEN_ERROR);
  }
}

/* ---------------------------------------------------------------- */
/*       WHEN EXECUTION IS INTERPRETED WE NEED TO SEND SOME ATTRINFO*/
/*       BACK TO LQH FOR LOGGING AND SENDING TO BACKUP AND STANDBY  */
/*       NODES.                                                     */
/*       INPUT:  LOG_ATTRINFOPTR         WHERE TO FETCH DATA FROM   */
/*               TLOG_START              FIRST INDEX TO LOG         */
/*               TLOG_END                LAST INDEX + 1 TO LOG      */
/* ---------------------------------------------------------------- */
int Dbtup::sendLogAttrinfo(Signal* signal,
                           KeyReqStruct * req_struct,
                           Uint32 TlogSize,
                           Operationrec *  const regOperPtr)
{
  /* Copy from Log buffer to segmented section,
   * then attach to ATTRINFO and execute direct
   * to LQH
   */
  ndbrequire( TlogSize > 0 );
  ndbassert(!m_is_query_block);
  Uint32 longSectionIVal= RNIL;
  bool ok= appendToSection(longSectionIVal, 
      &clogMemBuffer[0],
      TlogSize);
  if (unlikely(!ok))
  {
    /* Resource error, abort transaction */
    terrorCode = ZSEIZE_ATTRINBUFREC_ERROR;
    tupkeyErrorLab(req_struct);
    return -1;
  }

  /* Send a TUP_ATTRINFO signal to LQH, which contains
   * the relevant user pointer and the attrinfo section's
   * IVAL
   */
  signal->theData[0]= regOperPtr->userpointer;
  signal->theData[1]= TlogSize;
  signal->theData[2]= longSectionIVal;

  c_lqh->execTUP_ATTRINFO(signal);
  return 0;
}

inline
Uint32 
Dbtup::brancher(Uint32 TheInstruction, Uint32 TprogramCounter)
{         
  Uint32 TbranchDirection= TheInstruction >> 31;
  Uint32 TbranchLength= (TheInstruction >> 16) & 0x7fff;
  TprogramCounter--;
  if (TbranchDirection == 1)
  {
    jamDebug();
    /* ---------------------------------------------------------------- */
    /*       WE JUMP BACKWARDS.                                         */
    /* ---------------------------------------------------------------- */
    return (TprogramCounter - TbranchLength);
  }
  else
  {
    jamDebug();
    /* ---------------------------------------------------------------- */
    /*       WE JUMP FORWARD.                                           */
    /* ---------------------------------------------------------------- */
    return (TprogramCounter + TbranchLength);
  }
}

const Uint32 *
Dbtup::lookupInterpreterParameter(Uint32 paramNo,
                                  const Uint32 *subptr) const
{
  /**
   * The parameters...are stored in the subroutine section
   *
   * WORD2         WORD3       WORD4         WORD5
   * [ P0 HEADER ] [ P0 DATA ] [ P1 HEADER ] [ P1 DATA ]
   *
   * len=4 <=> 1 word
   */
  const Uint32 sublen = *subptr;
  ndbassert(sublen > 0);

  Uint32 pos = 1;
  while (paramNo)
  {
    const Uint32 * head = subptr + pos;
    const Uint32 len = AttributeHeader::getDataSize(* head);
    paramNo --;
    pos += 1 + len;
    if (unlikely(pos >= sublen))
      return nullptr;
  }

  const Uint32 * head = subptr + pos;
  const Uint32 len = AttributeHeader::getDataSize(* head);
  if (unlikely(pos + 1 + len > sublen))
    return nullptr;

  return head;
}

#define HEAP_MEMORY_SIZE_DWORDS 8200
#define MAX_HEAP_OFFSET 65535
#define NULL_INDICATOR 0
#define NOT_NULL_INDICATOR 1
int Dbtup::interpreterNextLab(Signal* signal,
                              KeyReqStruct* req_struct,
                              Uint32* logMemory,
                              Uint32* mainProgram,
                              Uint32 TmainProgLen,
                              Uint32* subroutineProg,
                              Uint32 TsubroutineLen,
                              Uint32 * tmpArea,
                              Uint32 tmpAreaSz)
{
  Uint32 theRegister;
  Uint32 theInstruction;
  Uint32 TprogramCounter= 0;
  Uint32* TcurrentProgram= mainProgram;
  Uint32 TcurrentSize= TmainProgLen;
  Uint32 TdataWritten= 0;
  Uint32 RstackPtr= 0;
  char *TheapMemoryChar;
  union {
    Uint32 TregMemBuffer[32];
    Uint64 align[16];
  };
  (void)align; // kill warning
  Uint32 TstackMemBuffer[32];

  TheapMemoryChar = (char*)&cheapMemory[0];

  Uint32& RnoOfInstructions = req_struct->no_exec_instructions;
  ndbassert(RnoOfInstructions == 0);
  /* ---------------------------------------------------------------- */
  // Initialise all 8 registers to contain the NULL value.
  // In this version we can handle 32 and 64 bit unsigned integers.
  // They are handled as 64 bit values. Thus the 32 most significant
  // bits are zeroed for 32 bit values.
  /* ---------------------------------------------------------------- */
  TregMemBuffer[0]= NULL_INDICATOR;
  TregMemBuffer[4]= NULL_INDICATOR;
  TregMemBuffer[8]= NULL_INDICATOR;
  TregMemBuffer[12]= NULL_INDICATOR;
  TregMemBuffer[16]= NULL_INDICATOR;
  TregMemBuffer[20]= NULL_INDICATOR;
  TregMemBuffer[24]= NULL_INDICATOR;
  TregMemBuffer[28]= NULL_INDICATOR;
  Uint32 tmpHabitant= ~0;

#ifdef TRACE_INTERPRETER
  g_eventLogger->info("Pogram size: %u", TcurrentSize);
#endif
  while (RnoOfInstructions < 16000) {
    /* ---------------------------------------------------------------- */
    /* EXECUTE THE NEXT INTERPRETER INSTRUCTION.                        */
    /* ---------------------------------------------------------------- */
    RnoOfInstructions++;
    theInstruction= TcurrentProgram[TprogramCounter];
    theRegister= Interpreter::getReg1(theInstruction) << 2;
#ifdef TRACE_INTERPRETER
    g_eventLogger->info(
        "Interpreter :"
        " RnoOfInstructions : %u.  TprogramCounter : %u.  Opcode : %u",
        RnoOfInstructions, TprogramCounter,
        Interpreter::getOpCode(theInstruction));
#endif
    if (TprogramCounter < TcurrentSize)
    {
      TprogramCounter++;
      switch (Interpreter::getOpCode(theInstruction)) {
      case Interpreter::READ_ATTR_INTO_REG:
      {
	jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
	/* ---------------------------------------------------------------- */
	// Read an attribute from the tuple into a register.
	// While reading an attribute we allow the attribute to be an array
	// as long as it fits in the 64 bits of the register.
	/* ---------------------------------------------------------------- */
	{
	  Uint32 theAttrinfo= (theInstruction & 0xFFFF0000);
	  int TnoDataRW= readAttributes(req_struct,
				     &theAttrinfo,
				     (Uint32)1,
				     &TregMemBuffer[theRegister],
				     (Uint32)3);
	  if (TnoDataRW == 2)
          {
            /* ------------------------------------------------------------- */
            // Two words read means that we get the instruction plus one 32 
            // word read. Thus we set the register to be a 32 bit register.
            /* ------------------------------------------------------------- */
            TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
            // arithmetic conversion if big-endian
            * (Int64*)(TregMemBuffer+theRegister+2)= TregMemBuffer[theRegister+1];
          }
          else if (TnoDataRW == 3)
          {
            /* ------------------------------------------------------------- */
            // Three words read means that we get the instruction plus two 
            // 32 words read. Thus we set the register to be a 64 bit register.
            /* ------------------------------------------------------------- */
            TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
            TregMemBuffer[theRegister+3]= TregMemBuffer[theRegister+2];
            TregMemBuffer[theRegister+2]= TregMemBuffer[theRegister+1];
          }
          else if (TnoDataRW == 1)
          {
            /* ------------------------------------------------------------- */
            // One word read means that we must have read a NULL value. We set
            // the register to indicate a NULL value.
            /* ------------------------------------------------------------- */
            TregMemBuffer[theRegister]= NULL_INDICATOR;
            TregMemBuffer[theRegister + 2]= 0;
            TregMemBuffer[theRegister + 3]= 0;
          }
          else if (TnoDataRW < 0)
          {
            jamDebug();
            terrorCode = Uint32(-TnoDataRW);
            tupkeyErrorLab(req_struct);
            return -1;
          }
          else
          {
            /* ------------------------------------------------------------- */
            // Any other return value from the read attribute here is not 
            // allowed and will lead to a system crash.
            /* ------------------------------------------------------------- */
            ndbabort();
          }
          break;
        }
      }
      case Interpreter::WRITE_ATTR_FROM_REG:
      {
        jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
        Uint32 TattrId = theInstruction >> 16;
        Uint32 TattrDescrIndex = (TattrId * ZAD_SIZE);
        Uint32 TregType= TregMemBuffer[theRegister];

        if (unlikely(TattrId >= req_struct->tablePtrP->m_no_of_attributes))
        {
          return TUPKEY_abort(req_struct, ZATTRIBUTE_ID_ERROR);
        }
        Uint32 TattrDesc1 =
          req_struct->tablePtrP->tabDescriptor[TattrDescrIndex];
        if (unlikely((TregType == NULL_INDICATOR)))
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        /* --------------------------------------------------------------- */
        // Calculate the number of words of this attribute.
        // We allow writes into arrays as long as they fit into the 64 bit
        // register size.
        /* --------------------------------------------------------------- */
        Uint32 TattrNoOfWords = AttributeDescriptor::getSizeInWords(TattrDesc1);
        Uint32 Toptype = req_struct->operPtrP->op_type;
        Uint32 TdataForUpdate[3];
        Uint32 Tlen;

        AttributeHeader ah(TattrId, TattrNoOfWords << 2);
        TdataForUpdate[0]= ah.m_value;
        TdataForUpdate[1]= TregMemBuffer[theRegister + 2];
        TdataForUpdate[2]= TregMemBuffer[theRegister + 3];
        Tlen= TattrNoOfWords + 1;
        if (Toptype == ZUPDATE)
        {
          if (TattrNoOfWords <= 2)
          {
            if (TattrNoOfWords == 1)
            {
              // arithmetic conversion if big-endian
              Int64 * tmp = new (&TregMemBuffer[theRegister + 2]) Int64;
              TdataForUpdate[1] = Uint32(* tmp);
              TdataForUpdate[2] = 0;
            }
            if (TregType == 0)
            {
              /* --------------------------------------------------------- */
              // Write a NULL value into the attribute
              /* --------------------------------------------------------- */
              ah.setNULL();
              TdataForUpdate[0]= ah.m_value;
              Tlen= 1;
            }
            int TnoDataRW= updateAttributes(req_struct,
              &TdataForUpdate[0],
              Tlen);
            if (TnoDataRW >= 0)
            {
              /* --------------------------------------------------------- */
              // Write the written data also into the log buffer so that it 
              // will be logged.
              /* --------------------------------------------------------- */
              logMemory[TdataWritten + 0]= TdataForUpdate[0];
              logMemory[TdataWritten + 1]= TdataForUpdate[1];
              logMemory[TdataWritten + 2]= TdataForUpdate[2];
              TdataWritten += Tlen;
            }
            else
            {
              terrorCode = Uint32(-TnoDataRW);
              tupkeyErrorLab(req_struct);
              return -1;
            }
          }
          else
          {
            return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZTRY_TO_UPDATE_ERROR);
        }
        break;
      }
      case Interpreter::WRITE_ATTR_FROM_MEM:
      {
        jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
        Uint32 attrId = theInstruction >> 16;
        Uint32 attrDescrIndex = (attrId * ZAD_SIZE);
        Uint32 TsizeRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TregOffsetType= TregMemBuffer[theRegister];
        Uint32 TregSizeType= TregMemBuffer[TsizeRegister];
        Int64 Toffset= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tsize= * (Int64*)(TregMemBuffer + TsizeRegister + 2);
        Uint32 Toptype = req_struct->operPtrP->op_type;

        if (unlikely(attrId >= req_struct->tablePtrP->m_no_of_attributes))
        {
          return TUPKEY_abort(req_struct, ZATTRIBUTE_ID_ERROR);
        }
        Uint32 attrDesc1 =
          req_struct->tablePtrP->tabDescriptor[attrDescrIndex];
        Uint32 attrNoOfBytes = AttributeDescriptor::getSizeInBytes(attrDesc1);
        if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                     (TregSizeType == NULL_INDICATOR)))
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(((Toffset + Tsize) > MAX_HEAP_OFFSET) ||
                      ((Toffset & 3) != 0) ||
                      (Toffset < 0)))
        {
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        if (unlikely((Tsize < 0) ||
                     (Tsize > attrNoOfBytes)))
        {
          return TUPKEY_abort(req_struct, ZWRITE_SIZE_TOO_BIG_ERROR);
        }
        if (unlikely(Toptype != ZUPDATE))
        {
          return TUPKEY_abort(req_struct, ZTRY_TO_UPDATE_ERROR);
        }
        /**
         * Tsize == 0 means writing a NULL value into the column and
         * AttributeHeader interprets size == 0 as NULL.
         */
        AttributeHeader ah(attrId, Tsize);
        Uint32* memory_ptr = (Uint32*)&TheapMemoryChar[Toffset];
        Uint32 words = 1 + (Tsize + 3) / 4;
        memory_ptr[0] = ah.m_value;
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("WRITE_ATTR_FROM_MEM: Tsize: %lld"
                            ", words: %u, offset: %lld",
                            Tsize,
                            words,
                            Toffset);
#endif
        int TnoDataRW = updateAttributes(req_struct,
                                        memory_ptr,
                                        words);
        if (TnoDataRW >= 0)
        {
          /**
           * Write the written data to the log memory for further
           * copying to the REDO log.
           */
          memcpy(&logMemory[TdataWritten],
                 memory_ptr,
                 words << 2);
          TdataWritten += words;
        }
        else
        {
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
        break;
      }
      case Interpreter::APPEND_ATTR_FROM_MEM:
      {
        jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
        Uint32 attrId = theInstruction >> 16;
        Uint32 TsizeRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 attrDescrIndex = (attrId * ZAD_SIZE);
        Uint32 TregOffsetType= TregMemBuffer[theRegister];
        Uint32 TregSizeType= TregMemBuffer[TsizeRegister];
        Int64 Toffset= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tsize= * (Int64*)(TregMemBuffer + TsizeRegister + 2);
        Uint32 Toptype = req_struct->operPtrP->op_type;

        if (unlikely(attrId >= req_struct->tablePtrP->m_no_of_attributes))
        {
          return TUPKEY_abort(req_struct, ZATTRIBUTE_ID_ERROR);
        }
        Uint32 attrDesc1 =
          req_struct->tablePtrP->tabDescriptor[attrDescrIndex];
        Uint32 attrNoOfBytes = AttributeDescriptor::getSizeInBytes(attrDesc1);
        Uint32 array = AttributeDescriptor::getArrayType(attrDesc1);
        if (unlikely((TregOffsetType == NULL_INDICATOR) ||
                     (TregSizeType == NULL_INDICATOR)))
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(((Toffset + Tsize) > MAX_HEAP_OFFSET) ||
                      ((Toffset & Int64(3)) != 0) ||
                      (Toffset < Int64(0))))
        {
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        if (unlikely((Tsize < Int64(0)) ||
                     (Tsize > Int64(attrNoOfBytes))))
        {
          return TUPKEY_abort(req_struct, ZWRITE_SIZE_TOO_BIG_ERROR);
        }
        /**
         * Append to a column is only allowed with non-NULL, it doesn't
         * make sense to append NULL to a column.
         */
        if (unlikely(Tsize == Int64(0)))
        {
          return TUPKEY_abort(req_struct, ZAPPEND_NULL_ERROR);
        }
        if (unlikely(Toptype != ZUPDATE))
        {
          return TUPKEY_abort(req_struct, ZTRY_TO_UPDATE_ERROR);
        }
        if ((array != NDB_ARRAYTYPE_SHORT_VAR) &&
            (array != NDB_ARRAYTYPE_MEDIUM_VAR))
        {
          return TUPKEY_abort(req_struct, ZAPPEND_ON_FIXED_SIZE_COLUMN_ERROR);
        }
        AttributeHeader ah(attrId, Tsize);
        ah.setPartialReadWriteFlag();
        Uint32* memory_ptr = (Uint32*)&TheapMemoryChar[Toffset];
        Uint32 words = 1 + (Tsize + 3) / 4;
        memory_ptr[0] = ah.m_value;
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("Toffset: %lld, Tsize: %lld, words: %u",
                            Toffset, Tsize, words);
#endif
        int TnoDataRW = updateAttributes(req_struct,
                                         memory_ptr,
                                         words);
        if (TnoDataRW >= 0)
        {
          /**
           * Write the written data to the log memory for further
           * copying to the REDO log.
           */
          memcpy(&logMemory[TdataWritten],
                 memory_ptr,
                 words << 2);
          TdataWritten += words;
        }
        else
        {
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
        break;
      }
      case Interpreter::READ_PARTIAL_ATTR_TO_MEM:
      {
        jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
        Uint32 ToffsetType= TregMemBuffer[theRegister];
        Int64 Toffset= * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 TposRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TsizeRegister= Interpreter::getReg4(theInstruction) << 2;
        Uint32 TposType= TregMemBuffer[TposRegister];
        Uint32 TsizeType= TregMemBuffer[TsizeRegister];
        if (unlikely((ToffsetType == NULL_INDICATOR) ||
                     (TposType == NULL_INDICATOR) ||
                     (TsizeType == NULL_INDICATOR)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Reg %u or %u or %u NULL, LINE: %u",
                              theRegister >> 2,
                              TposRegister >> 2,
                              TsizeRegister >> 2,
                              __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(Toffset < 0 ||
                     (Toffset > ((HEAP_MEMORY_SIZE_DWORDS * 8) -
                      (MAX_TUPLE_SIZE_IN_WORDS * 4))) ||
                     ((Toffset & Int64(3)) != 0)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Offset %lld isn't ok, %u", Toffset, __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        Uint32 memory_offset = Uint32(Toffset);
        Int64 Tpos= * (Int64*)(TregMemBuffer + TposRegister + 2);
        if (unlikely(Tpos < 0 || Tpos >= (MAX_TUPLE_SIZE_IN_WORDS * 4)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Pos %lld isn't ok, %u", Tpos, __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZPARTIAL_READ_ERROR);
        }
        Uint32 read_pos = (Uint32)Tpos;
        Int64 Tsize= * (Int64*)(TregMemBuffer + TsizeRegister + 2);
        if (unlikely(Tsize <= 0 || Tsize >= (MAX_TUPLE_SIZE_IN_WORDS * 4)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Size %lld isn't ok, %u", Tsize, __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZPARTIAL_READ_ERROR);
        }
        Uint32 read_size = (Uint32)Tsize;
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        Uint32 TattrId = theInstruction >> 19;
        AttributeHeader ah(TattrId, 0);
        ah.setPartialReadWriteFlag();
        Uint32 TdataForRead[2];
        TdataForRead[0] = ah.m_value;
        TdataForRead[1] = read_size | read_pos << 16;
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Partial read of attribute %u, line: %u",
                              TattrId, __LINE__);
#endif
        int TnoDataRW= readAttributes(req_struct,
                                     &TdataForRead[0],
                                     (Uint32)2,
                                     (Uint32*)&TheapMemoryChar[memory_offset],
                                     (Uint32)MAX_TUPLE_SIZE_IN_WORDS);
        if (TnoDataRW < 0)
        {
          jamDebug();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
        Uint32 *memory_ptr = (Uint32*)&TheapMemoryChar[memory_offset];
        Uint32 header = *memory_ptr;
        AttributeHeader ah_read(header);
        if (ah_read.isNULL())
        {
          TregMemBuffer[TdestRegister]= NULL_INDICATOR;
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("READ_PARTIAL_ATTR_TO_MEM: NULL");
#endif
        }
        else
        {
          Uint32 read_len = ah_read.getByteSize();
          * (Int64*)(TregMemBuffer+TdestRegister+2)= read_len;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("READ_PARTIAL_ATTR_TO_MEM:"
                              " Len: %u, offset: %u",
                              read_len,
                              memory_offset);
#endif
        }
        break; 
      }
      case Interpreter::READ_ATTR_TO_MEM:
      {
        jamDebug();
        RnoOfInstructions += 3; //A bit heavier instruction
        Uint32 ToffsetType= TregMemBuffer[theRegister];
        Int64 Toffset= * (Int64*)(TregMemBuffer + theRegister + 2);
	Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        Uint32 TattrId = theInstruction >> 16;
        Uint32 theAttrinfo = (TattrId << 16);
        if (unlikely((ToffsetType == NULL_INDICATOR)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Reg %u NULL, LINE: %u", theRegister >> 2, __LINE__);
#endif
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(Toffset < 0 ||
                     (Toffset > ((HEAP_MEMORY_SIZE_DWORDS * 8) -
                      (MAX_TUPLE_SIZE_IN_WORDS * 4))) ||
                     ((Toffset & Int64(3)) != 0)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Offset %lld isn't ok, %u", Toffset, __LINE__);
#endif
	  return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        Uint32 memory_offset = Uint32(Toffset);
        int TnoDataRW= readAttributes(req_struct,
                                     &theAttrinfo,
                                     (Uint32)1,
                                     (Uint32*)&TheapMemoryChar[memory_offset],
                                     (Uint32)MAX_TUPLE_SIZE_IN_WORDS);
        if (TnoDataRW < 0)
        {
          jamDebug();
          terrorCode = Uint32(-TnoDataRW);
          tupkeyErrorLab(req_struct);
          return -1;
        }
        Uint32 *memory_ptr = (Uint32*)&TheapMemoryChar[memory_offset];
        Uint32 header = *memory_ptr;
        AttributeHeader ah(header);
        if (ah.isNULL())
        {
          TregMemBuffer[TdestRegister]= NULL_INDICATOR;
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("READ_ATTR_TO_MEM: NULL");
#endif
        }
        else
        {
          Uint32 read_len = ah.getByteSize();
          * (Int64*)(TregMemBuffer+TdestRegister+2)= read_len;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("READ_ATTR_TO_MEM: Len: %u", read_len);
#endif
        }
        break;
      }
      case (Interpreter::LOAD_CONST_MEM + OVERFLOW_OPCODE):
      {
        /**
         * Write content of a register to an output register.
         * Can be read later with a read of a pseudo code.
         */
        Uint32 valueType= TregMemBuffer[theRegister];
        Uint32 outputInx = theInstruction >> 16;
	Int64 value = * (Int64*)(TregMemBuffer + theRegister + 2);
        if (unlikely((valueType == NULL_INDICATOR)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Reg %u NULL, LINE: %u", theRegister >> 2, __LINE__);
#endif
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        Int64 upper_value = value >> 32;
        if (upper_value != 0)
        {
	  return TUPKEY_abort(req_struct, ZVALUE_OVERFLOW_OUTPUT_REGISTER);
        }
        if (unlikely(outputInx >= AttributeHeader::MaxInterpreterOutputIndex))
        {
	  return TUPKEY_abort(req_struct, ZOUTPUT_INDEX_ERROR);
        }
        Uint32 value32 = Uint32(value);
        c_interpreter_output[outputInx] = value32;
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("write_interpreter_output[%u] = %u",
                            outputInx,
                            value32);
#endif
        break;
      }
      case Interpreter::CONVERT_SIZE:
      {
        /**
         * theRegister contains the memory offset of the two bytes
         * to be read.
         * tDestRegister is the register that will have the size
         * that is read and converted.
         */
        Uint32 offsetType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 TdestRegister = Interpreter::getReg2(theInstruction) << 2;
        if (unlikely((offsetType == NULL_INDICATOR)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Reg %u NULL, LINE: %u", theRegister >> 2, __LINE__);
#endif
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1) ||
                    (memoryOffset < 0)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        Uint32 low_byte = TheapMemoryChar[memoryOffset];
        Uint32 high_byte = TheapMemoryChar[memoryOffset + 1];
        Uint32 size_read = low_byte + (256 * high_byte);
	* (Int64*)(TregMemBuffer+TdestRegister+2)= (Int64)size_read;
	TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("convert_size: low_byte: %u, high_byte: %u,"
                            " offset: %lld, size_read: %u",
                            low_byte,
                            high_byte,
                            memoryOffset,
                            size_read);
#endif
        break;
      }
      case (Interpreter::CONVERT_SIZE + OVERFLOW_OPCODE):
      {
        /**
         * theRegister contains the memory offset of the two bytes
         * to be written.
         * tDestRegister is the register that will have the size
         * that is read and converted.
         */
        Uint32 TsizeRegister = Interpreter::getReg2(theInstruction) << 2;
        Uint32 offsetType= TregMemBuffer[theRegister];
        Uint32 sizeType= TregMemBuffer[TsizeRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        if (unlikely((offsetType == NULL_INDICATOR) ||
                     (sizeType == NULL_INDICATOR)))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1) ||
                    (memoryOffset < 0)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
	Int64 size = * (Int64*)(TregMemBuffer + TsizeRegister + 2);
        if (unlikely(size <= 0 || size >= (MAX_TUPLE_SIZE_IN_WORDS * 4)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Size %lld isn't ok, %u", size, __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZPARTIAL_READ_ERROR);
        }
        Uint32 low_byte = size & 255;
        Uint32 high_byte = size >> 8;
        TheapMemoryChar[memoryOffset] = low_byte;
        TheapMemoryChar[memoryOffset + 1] = high_byte;
#ifdef TRACE_INTERPRETER
        g_eventLogger->info("write_size_mem: low_byte: %u, high_byte: %u,"
                            " offset: %lld",
                            low_byte,
                            high_byte,
                            memoryOffset);
#endif
        break;
      }
      case Interpreter::READ_UINT8_MEM_TO_REG:
      {
        jamDebug();
        Uint32 memoryOffset = theInstruction >> 16;
        if (unlikely(memoryOffset > MAX_HEAP_OFFSET))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        Uint8 value = TheapMemoryChar[memoryOffset];
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+theRegister+2)= (Int64)value;
        break;
      }
      case Interpreter::READ_UINT16_MEM_TO_REG:
      {
        jamDebug();
        Uint32 memoryOffset = theInstruction >> 16;
        Uint16 value;
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 2);
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+theRegister+2)= (Int64)value;
        break;
      }
      case Interpreter::READ_UINT32_MEM_TO_REG:
      {
        jamDebug();
        Uint32 memoryOffset = theInstruction >> 16;
        Uint32 value;
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 4);
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+theRegister+2)= (Int64)value;
        break;
      }
      case Interpreter::READ_INT64_MEM_TO_REG:
      {
        jamDebug();
        Uint32 memoryOffset = theInstruction >> 16;
        Int64 value;
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 8);
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+theRegister+2)= value;
        break;
      }

      case (Interpreter::READ_UINT8_MEM_TO_REG + OVERFLOW_OPCODE):
      {
        jamDebug();
	Uint32 memoryOffsetType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 destRegister = Interpreter::getReg2(theInstruction) << 2;
	if (unlikely(memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 0)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        Uint8 value = TheapMemoryChar[memoryOffset];
	* (Int64*)(TregMemBuffer+destRegister+2)= (Int64)value;
	TregMemBuffer[destRegister]= NOT_NULL_INDICATOR;
        break;
      }
      case (Interpreter::READ_UINT16_MEM_TO_REG + OVERFLOW_OPCODE):
      {
        jamDebug();
	Uint32 memoryOffsetType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 destRegister = Interpreter::getReg2(theInstruction) << 2;
        Uint16 value;
	if (unlikely(memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 2);
	* (Int64*)(TregMemBuffer+destRegister+2)= (Int64)value;
	TregMemBuffer[destRegister]= NOT_NULL_INDICATOR;
        break;
      }
      case (Interpreter::READ_UINT32_MEM_TO_REG + OVERFLOW_OPCODE):
      {
        jamDebug();
	Uint32 memoryOffsetType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 destRegister = Interpreter::getReg2(theInstruction) << 2;
        Uint32 value;
	if (unlikely(memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 4);
	* (Int64*)(TregMemBuffer+destRegister+2)= (Int64)value;
	TregMemBuffer[destRegister]= NOT_NULL_INDICATOR;
        break;
      }
      case (Interpreter::READ_INT64_MEM_TO_REG + OVERFLOW_OPCODE):
      {
        jamDebug();
	Uint32 memoryOffsetType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 destRegister = Interpreter::getReg2(theInstruction) << 2;
        Int64 value;
	if (unlikely(memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&value, &TheapMemoryChar[memoryOffset], 8);
	* (Int64*)(TregMemBuffer+destRegister+2)= value;
	TregMemBuffer[destRegister]= NOT_NULL_INDICATOR;
        break;
      }
      case Interpreter::WRITE_UINT8_REG_TO_MEM:
      {
        jamDebug();
        Uint32 TregType= TregMemBuffer[theRegister];
        Uint32 memoryOffset = theInstruction >> 16;
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint8 val = (Uint8)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 0)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 1);
        break;
      }
      case Interpreter::WRITE_UINT16_REG_TO_MEM:
      {
        jamDebug();
        Uint32 TregType= TregMemBuffer[theRegister];
        Uint32 memoryOffset = theInstruction >> 16;
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint16 val = (Uint16)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 1)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 2);
        break;
      }
      case Interpreter::WRITE_UINT32_REG_TO_MEM:
      {
        jamDebug();
        Uint32 TregType= TregMemBuffer[theRegister];
        Uint32 memoryOffset = theInstruction >> 16;
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint32 val = (Uint32)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 3)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 4);
        break;
      }
      case Interpreter::WRITE_INT64_REG_TO_MEM:
      {
        jamDebug();
        Uint32 TregType= TregMemBuffer[theRegister];
        Uint32 memoryOffset = theInstruction >> 16;
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
	if (unlikely(TregType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &Tvalue, 8);
        break;
      }

      case (Interpreter::WRITE_UINT8_REG_TO_MEM + OVERFLOW_OPCODE):
      {
        jamDebug();
        Uint32 registerOffset = Interpreter::getReg2(theInstruction) << 2;
        Uint32 TregType= TregMemBuffer[theRegister];
	Uint32 memoryOffsetType= TregMemBuffer[registerOffset];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + registerOffset + 2);
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint8 val = (Uint8)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR ||
                     memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 1);
        break;
      }
      case (Interpreter::WRITE_UINT16_REG_TO_MEM + OVERFLOW_OPCODE):
      {
        jamDebug();
        Uint32 registerOffset = Interpreter::getReg2(theInstruction) << 2;
        Uint32 TregType= TregMemBuffer[theRegister];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + registerOffset + 2);
	Uint32 memoryOffsetType= TregMemBuffer[registerOffset];
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint16 val = (Uint16)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR ||
                     memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 2);
        break;
      }
      case (Interpreter::WRITE_UINT32_REG_TO_MEM + OVERFLOW_OPCODE):
      {
        jamDebug();
        Uint32 registerOffset = Interpreter::getReg2(theInstruction) << 2;
        Uint32 TregType= TregMemBuffer[theRegister];
	Uint32 memoryOffsetType= TregMemBuffer[registerOffset];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + registerOffset + 2);
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
        Uint32 val = (Uint32)Tvalue;
	if (unlikely(TregType == NULL_INDICATOR ||
                     memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &val, 4);
        break;
      }
      case (Interpreter::WRITE_INT64_REG_TO_MEM + OVERFLOW_OPCODE):
      {
        jamDebug();
        Uint32 registerOffset = Interpreter::getReg2(theInstruction) << 2;
        Uint32 TregType= TregMemBuffer[theRegister];
	Uint32 memoryOffsetType= TregMemBuffer[registerOffset];
	Int64 memoryOffset = * (Int64*)(TregMemBuffer + registerOffset + 2);
	Int64 Tvalue = * (Int64*)(TregMemBuffer+theRegister+2);
	if (unlikely(TregType == NULL_INDICATOR ||
                     memoryOffsetType == NULL_INDICATOR))
        {
	  return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(memoryOffset > (MAX_HEAP_OFFSET - 7)))
        {
          jam();
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
        }
        memcpy(&TheapMemoryChar[memoryOffset], &Tvalue, 8);
        break;
      }

      case Interpreter::LOAD_CONST_NULL:
      {
	jamDebug();
	TregMemBuffer[theRegister]= NULL_INDICATOR;
	break;
      }
      case Interpreter::LOAD_CONST16:
      {
	jamDebug();
	* (Int64*)(TregMemBuffer+theRegister+2)= theInstruction >> 16;
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	break;
      }
      case Interpreter::LOAD_CONST32:
      {
	jamDebug();
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+theRegister+2)= * 
	  (TcurrentProgram+TprogramCounter);
	TprogramCounter++;
	break;
      }
      case Interpreter::LOAD_CONST64:
      {
	jamDebug();
	TregMemBuffer[theRegister]= NOT_NULL_INDICATOR;
        TregMemBuffer[theRegister + 2 ]= * (TcurrentProgram +
                                             TprogramCounter++);
        TregMemBuffer[theRegister + 3 ]= * (TcurrentProgram +
                                             TprogramCounter++);
	break;
      }
      case Interpreter::LOAD_CONST_MEM:
      {
        RnoOfInstructions += 1; //A bit heavier instruction
        Uint32 registerDestSize = Interpreter::getReg2(theInstruction) << 2;
        Uint32 registerOffsetType= TregMemBuffer[theRegister];
        Uint32 Tsize = theInstruction >> 16;
        Uint32 words = (Tsize + 3) / 4;
        Int64 Toffset = * (Int64*)(TregMemBuffer + theRegister + 2);
        if (unlikely(registerOffsetType == NULL_INDICATOR))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Line %u, Register init error", __LINE__);
#endif
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        if (unlikely(((Toffset + Int64(words << 2)) > MAX_HEAP_OFFSET) ||
                      ((Toffset & Int64(3)) != 0) ||
                      (Toffset < Int64(0))))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Line %u, Offset error: %lld", __LINE__, Toffset);
          return TUPKEY_abort(req_struct, ZMEMORY_OFFSET_ERROR);
#endif
        }
        if (unlikely(Tsize > (MAX_TUPLE_SIZE_IN_WORDS * 4)))
        {
#ifdef TRACE_INTERPRETER
          g_eventLogger->info("Line %u, Size error: %u", __LINE__, Tsize);
#endif
          return TUPKEY_abort(req_struct, ZLOAD_MEM_TOO_BIG_ERROR);
        }
	TregMemBuffer[registerDestSize]= NOT_NULL_INDICATOR;
	* (Int64*)(TregMemBuffer+registerDestSize+2) = (Int64)Tsize;
        Uint32* memory_ptr = (Uint32*)&TheapMemoryChar[Toffset];
        memcpy(memory_ptr,
               &TcurrentProgram[TprogramCounter],
               words << 2);
        zero32((Uint8*)memory_ptr, Tsize);
        TprogramCounter += words;
	break;
      }
      case Interpreter::ADD_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 + Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::ADD_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        //Backwards compatability, use Reg4
        Uint32 TdestRegister= Interpreter::getReg4(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 + Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::SUB_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 - Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::SUB_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        //Backwards compatability, use Reg4
        Uint32 TdestRegister= Interpreter::getReg4(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Int64 Tdest0= Tleft0 - Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::LSHIFT_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          if (likely(Tright0 <= 64 && Tright0 >= 0))
          {
            Uint64 Tdest0= Tleft0 << Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZSHIFT_OPERAND_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::LSHIFT_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          if (likely(Tright0 <= 64 && Tright0 >= 0))
          {
            Uint64 Tdest0= Tleft0 << Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZSHIFT_OPERAND_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::RSHIFT_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          if (likely(Tright0 <= 64 && Tright0 >= 0))
          {
            Uint64 Tdest0= Tleft0 >> Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZSHIFT_OPERAND_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::RSHIFT_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          if (likely(Tright0 <= 64 && Tright0 >= 0))
          {
            Uint64 Tdest0= Tleft0 >> Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZSHIFT_OPERAND_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::MUL_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 * Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::MUL_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 * Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::DIV_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          if (likely(Tright0 != 0))
          {
            Uint64 Tdest0= Tleft0 / Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZDIV_BY_ZERO_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::DIV_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          if (likely(Tright0 != 0))
          {
            Uint64 Tdest0= Tleft0 / Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZDIV_BY_ZERO_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::AND_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 & Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::AND_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 & Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::OR_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 | Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::OR_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 | Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::XOR_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 ^ Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::XOR_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          Uint64 Tdest0= Tleft0 ^ Tright0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::MOD_CONST_REG_TO_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= (Int64)(theInstruction >> 16);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          if (likely(Tright0 != 0))
          {
            Uint64 Tdest0= Tleft0 % Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZDIV_BY_ZERO_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::MOD_REG_REG:
      {
	jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely((TleftType & TrightType) != NULL_INDICATOR))
        {
          if (likely(Tright0 != 0))
          {
            Uint64 Tdest0= Tleft0 % Tright0;
            TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
            * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
          }
          else
          {
            return TUPKEY_abort(req_struct, ZDIV_BY_ZERO_ERROR);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::NOT_REG_REG:
      {
	jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Uint32 TdestRegister= Interpreter::getReg3(theInstruction) << 2;
        if (likely(TleftType != NULL_INDICATOR))
        {
          Uint64 Tdest0= ~Tleft0;
          TregMemBuffer[TdestRegister]= NOT_NULL_INDICATOR;
          * (Int64*)(TregMemBuffer+TdestRegister+2)= Tdest0;
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH:
      {
	TprogramCounter= brancher(theInstruction, TprogramCounter);
	break;
      }
      case Interpreter::BRANCH_REG_EQ_NULL:
      {
	if (TregMemBuffer[theRegister] != NULL_INDICATOR)
        {
	  jamDebug();
	  continue;
	}
        else
        {
	  jamDebug();
	  TprogramCounter= brancher(theInstruction, TprogramCounter);
	}
	break;
      }
      case Interpreter::BRANCH_REG_NE_NULL:
      {
	if (TregMemBuffer[theRegister] == NULL_INDICATOR)
        {
	  jamDebug();
	  continue;
	}
        else
        {
	  jamDebug();
	  TprogramCounter= brancher(theInstruction, TprogramCounter);
	}
	break;
      }
      case Interpreter::BRANCH_EQ_REG_REG:
      {
        jamDebug();
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        if ((TrightType & TleftType) != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 == Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_NE_REG_REG:
      {
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        if ((TrightType & TleftType) != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 != Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_LT_REG_REG:
      {
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        if ((TrightType & TleftType) != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 < Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_LE_REG_REG:
      {
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        if ((TrightType & TleftType) != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 <= Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_GT_REG_REG:
      {
        Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
        Uint32 TleftType= TregMemBuffer[theRegister];
        Uint32 TrightType= TregMemBuffer[TrightRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
        if ((TrightType & TleftType) != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 > Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_GE_REG_REG:
      {
         Uint32 TrightRegister= Interpreter::getReg2(theInstruction) << 2;
         Uint32 TleftType= TregMemBuffer[theRegister];
         Uint32 TrightType= TregMemBuffer[TrightRegister];
         Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
         Int64 Tright0= * (Int64*)(TregMemBuffer + TrightRegister + 2);
         if ((TrightType & TleftType) != NULL_INDICATOR)
         {
           jamDebug();
           if (Tleft0 >= Tright0)
           {
             TprogramCounter= brancher(theInstruction, TprogramCounter);
           }
         }
         else
         {
           return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
         }
         break;
       }
      case (Interpreter::BRANCH_EQ_REG_REG + OVERFLOW_OPCODE):
      {
        jamDebug();
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 == Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case (Interpreter::BRANCH_NE_REG_REG + OVERFLOW_OPCODE):
      {
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 != Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case (Interpreter::BRANCH_LT_REG_REG + OVERFLOW_OPCODE):
      {
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 < Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case (Interpreter::BRANCH_LE_REG_REG + OVERFLOW_OPCODE):
      {
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 <= Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case (Interpreter::BRANCH_GT_REG_REG + OVERFLOW_OPCODE):
      {
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 > Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case (Interpreter::BRANCH_GE_REG_REG + OVERFLOW_OPCODE):
      {
        Uint32 TleftType= TregMemBuffer[theRegister];
        Int64 Tleft0= * (Int64*)(TregMemBuffer + theRegister + 2);
        Int64 Tright0 = Int64(((theInstruction >> 9) & 0x3F));
        if (TleftType != NULL_INDICATOR)
        {
          jamDebug();
          if (Tleft0 >= Tright0)
          {
            TprogramCounter= brancher(theInstruction, TprogramCounter);
          }
        }
        else
        {
          return TUPKEY_abort(req_struct, ZREGISTER_INIT_ERROR);
        }
        break;
      }
      case Interpreter::BRANCH_ATTR_OP_ATTR:
      case Interpreter::BRANCH_ATTR_OP_PARAM:
      case Interpreter::BRANCH_ATTR_OP_ARG:
      case (Interpreter::BRANCH_ATTR_OP_ATTR + OVERFLOW_OPCODE):
      case (Interpreter::BRANCH_ATTR_OP_PARAM + OVERFLOW_OPCODE):
      case (Interpreter::BRANCH_ATTR_OP_ARG + OVERFLOW_OPCODE):
      {
        jamDebug();
        const Uint32 ins2 = TcurrentProgram[TprogramCounter];
        Uint32 attrId = Interpreter::getBranchCol_AttrId(ins2) << 16;
        const Uint32 opCode = Interpreter::getOpCode(theInstruction);

        if (tmpHabitant != attrId)
        {
	  Int32 TnoDataR = readAttributes(req_struct,
					  &attrId, 1,
					  tmpArea, tmpAreaSz);
	  
	  if (unlikely(TnoDataR < 0))
          {
	    jam();
            terrorCode = Uint32(-TnoDataR);
	    tupkeyErrorLab(req_struct);
	    return -1;
	  }
	  tmpHabitant= attrId;
        }

        // get type
	attrId >>= 16;
	const Uint32* attrDescriptor = req_struct->tablePtrP->tabDescriptor +
	  (attrId * ZAD_SIZE);
	const Uint32 TattrDesc1 = attrDescriptor[0];
	const Uint32 TattrDesc2 = attrDescriptor[1];
	const Uint32 typeId = AttributeDescriptor::getType(TattrDesc1);
	const CHARSET_INFO *cs = nullptr;
	if (AttributeOffset::getCharsetFlag(TattrDesc2))
	{
	  const Uint32 pos = AttributeOffset::getCharsetPos(TattrDesc2);
	  cs = req_struct->tablePtrP->charsetArray[pos];
	}
	const NdbSqlUtil::Type& sqlType = NdbSqlUtil::getType(typeId);

        // get data for 1st argument, always an ATTR.
        const AttributeHeader ah(tmpArea[0]);
        const char* s1 = (char*)&tmpArea[1];
        // fixed length in 5.0
        Uint32 attrLen = AttributeDescriptor::getSizeInBytes(TattrDesc1);
        if (unlikely(typeId == NDB_TYPE_BIT))
        {
          /* Size in bytes for bit fields can be incorrect due to
           * rounding down
           */
          Uint32 bitFieldAttrLen= (AttributeDescriptor::getArraySize(TattrDesc1)
                                   + 7) / 8;
          attrLen= bitFieldAttrLen;
        }

	// 2'nd argument, literal, parameter or another attribute
        Uint32 argLen = 0;
        Uint32 step = 0;
        const char* s2 = nullptr;

        if (likely(opCode == Interpreter::BRANCH_ATTR_OP_ARG))
        {
          // Compare ATTR with a literal value given by interpreter code
          jamDebug();
          argLen = Interpreter::getBranchCol_Len(ins2);
          step = argLen;
          s2 = (char*)&TcurrentProgram[TprogramCounter+1];
        }
        else if (opCode == Interpreter::BRANCH_ATTR_OP_PARAM)
        {
          // Compare ATTR with a parameter
          jamDebug();
          ndbassert(req_struct != nullptr);
          ndbassert(req_struct->operPtrP != nullptr);

          const Uint32 paramNo = Interpreter::getBranchCol_ParamNo(ins2);
          const Uint32 *paramPos = subroutineProg;
          const Uint32 *paramptr = lookupInterpreterParameter(paramNo,
                                                              paramPos);
          if (unlikely(paramptr == nullptr))
          {
            jam();
            terrorCode = 99; // TODO
            tupkeyErrorLab(req_struct);
            return -1;
          }

          argLen = AttributeHeader::getByteSize(* paramptr);
          step = 0;
          s2 = (char*)(paramptr + 1);
        }
        else if (opCode == Interpreter::BRANCH_ATTR_OP_ATTR)
        {
          // Compare ATTR with another ATTR
          jamDebug();
          Uint32 attr2Id = Interpreter::getBranchCol_AttrId2(ins2) << 16;

          // Attr2 to be read into tmpArea[] after Attr1.
          const Uint32 firstAttrWords = attrLen+1;
          assert(tmpAreaSz >= 2*firstAttrWords);
          Int32 TnoDataR = readAttributes(req_struct,
                                          &attr2Id, 1,
                                          &tmpArea[firstAttrWords],
                                          tmpAreaSz-firstAttrWords);
          if (unlikely(TnoDataR < 0))
          {
            jam();
            terrorCode = Uint32(-TnoDataR);
            tupkeyErrorLab(req_struct);
            return -1;
          }

          const AttributeHeader ah2(tmpArea[firstAttrWords]);
          if (!ah2.isNULL())
          {
            // Get type
            attr2Id >>= 16;
            const Uint32* attr2Descriptor = req_struct->tablePtrP->tabDescriptor +
              (attr2Id * ZAD_SIZE);
            const Uint32 Tattr2Desc1 = attr2Descriptor[0];
            const Uint32 type2Id = AttributeDescriptor::getType(Tattr2Desc1);

            argLen = AttributeDescriptor::getSizeInBytes(Tattr2Desc1);
            if (unlikely(type2Id == NDB_TYPE_BIT))
            {
              /* Size in bytes for bit fields can be incorrect due to
               * rounding down
               */
              Uint32 bitFieldAttrLen= (AttributeDescriptor::getArraySize(Tattr2Desc1)
                                       + 7) / 8;
              argLen= bitFieldAttrLen;
            }
            s2 = (char*)&tmpArea[firstAttrWords+1];
          }
          step = 0;
        } //!ah2.isNULL()

        // Evaluate
        const bool r1_null = ah.isNULL();
        const bool r2_null = argLen == 0;
        if (r1_null || r2_null)
        {
          // There are NULL-valued operands, check the NullSemantics
          const Uint32 nullSemantics =
              Interpreter::getNullSemantics(theInstruction);
          if (nullSemantics == Interpreter::IF_NULL_BREAK_OUT)
          {
            // Branch out of AND conjunction
            TprogramCounter = brancher(theInstruction, TprogramCounter);
            break;
          }
          if (nullSemantics == Interpreter::IF_NULL_CONTINUE)
          {
            // Ignore NULL in OR conjunction,  -> next instruction
            const Uint32 tmp = ((step + 3) >> 2) + 1;
            TprogramCounter += tmp;
            break;
          }
        }

        const Uint32 cond = Interpreter::getBinaryCondition(theInstruction);
        int res1;
        if (cond <= Interpreter::GE)
        {
          /* Inequality - EQ, NE, LT, LE, GT, GE */
          if (r1_null || r2_null)
          {
            // NULL==NULL and NULL<not-NULL
            res1 = r1_null && r2_null ? 0 : r1_null ? -1 : 1;
          }
          else
          {
	    jamDebug();
	    if (unlikely(sqlType.m_cmp == 0))
	    {
	      return TUPKEY_abort(req_struct, ZUNSUPPORTED_BRANCH);
	    }
            res1 = (*sqlType.m_cmp)(cs, s1, attrLen, s2, argLen);
          }
	}
        else
        {
          if ((cond == Interpreter::LIKE) ||
              (cond == Interpreter::NOT_LIKE))
          {
            if (r1_null || r2_null)
            {
              // NULL like NULL is true (has no practical use)
              res1 =  r1_null && r2_null ? 0 : -1;
            }
            else
            {
              jam();
              if (unlikely(sqlType.m_like == 0))
              {
                return TUPKEY_abort(req_struct, ZUNSUPPORTED_BRANCH);
              }
              res1 = (*sqlType.m_like)(cs, s1, attrLen, s2, argLen);
            }
          }
          else
          {
            /* AND_XX_MASK condition */
            ndbassert(cond <= Interpreter::AND_NE_ZERO);
            if (unlikely(sqlType.m_mask == 0))
            {
              return TUPKEY_abort(req_struct, ZUNSUPPORTED_BRANCH);
            }
            /* If either arg is NULL, we say COL AND MASK
             * NE_ZERO and NE_MASK.
             */
            if (r1_null || r2_null)
            {
              res1= 1;
            }
            else
            {
              
              bool cmpZero= 
                (cond == Interpreter::AND_EQ_ZERO) ||
                (cond == Interpreter::AND_NE_ZERO);
              
              res1 = (*sqlType.m_mask)(s1, attrLen, s2, argLen, cmpZero);
            }
          }
        }

        int res = 0;
        switch ((Interpreter::BinaryCondition)cond) {
        case Interpreter::EQ:
          res = (res1 == 0);
          break;
        case Interpreter::NE:
          res = (res1 != 0);
          break;
        // note the condition is backwards
        case Interpreter::LT:
          res = (res1 > 0);
          break;
        case Interpreter::LE:
          res = (res1 >= 0);
          break;
        case Interpreter::GT:
          res = (res1 < 0);
          break;
        case Interpreter::GE:
          res = (res1 <= 0);
          break;
        case Interpreter::LIKE:
          res = (res1 == 0);
          break;
        case Interpreter::NOT_LIKE:
          res = (res1 == 1);
          break;
        case Interpreter::AND_EQ_MASK:
          res = (res1 == 0);
          break;
        case Interpreter::AND_NE_MASK:
          res = (res1 != 0);
          break;
        case Interpreter::AND_EQ_ZERO:
          res = (res1 == 0);
          break;
        case Interpreter::AND_NE_ZERO:
          res = (res1 != 0);
          break;
	  // XXX handle invalid value
        }
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(
            "cond=%u attr(%d)='%.*s'(%d) str='%.*s'(%d) res1=%d res=%d", cond,
            attrId >> 16, attrLen, s1, attrLen, argLen, s2, argLen, res1, res);
#endif
        if (res)
          TprogramCounter = brancher(theInstruction, TprogramCounter);
        else 
	{
          Uint32 tmp = ((step + 3) >> 2) + 1;
          TprogramCounter += tmp;
        }
	break;
      }
	
      case Interpreter::BRANCH_ATTR_EQ_NULL:{
	jamDebug();
	Uint32 ins2= TcurrentProgram[TprogramCounter];
	Uint32 attrId= Interpreter::getBranchCol_AttrId(ins2) << 16;
	
	if (tmpHabitant != attrId)
        {
	  Int32 TnoDataR= readAttributes(req_struct,
					  &attrId, 1,
					  tmpArea, tmpAreaSz);
	  
	  if (unlikely(TnoDataR < 0))
          {
	    jam();
            terrorCode = Uint32(-TnoDataR);
	    tupkeyErrorLab(req_struct);
	    return -1;
	  }
	  tmpHabitant= attrId;
	}
	
	AttributeHeader ah(tmpArea[0]);
	if (ah.isNULL())
        {
	  TprogramCounter= brancher(theInstruction, TprogramCounter);
	}
        else
        {
	  TprogramCounter ++;
	}
	break;
      }

      case Interpreter::BRANCH_ATTR_NE_NULL:
      {
	jamDebug();
	Uint32 ins2= TcurrentProgram[TprogramCounter];
	Uint32 attrId= Interpreter::getBranchCol_AttrId(ins2) << 16;
	
	if (tmpHabitant != attrId)
        {
	  Int32 TnoDataR= readAttributes(req_struct,
					  &attrId, 1,
					  tmpArea, tmpAreaSz);
	  
	  if (unlikely(TnoDataR < 0))
          {
	    jam();
            terrorCode = Uint32(-TnoDataR);
	    tupkeyErrorLab(req_struct);
	    return -1;
	  }
	  tmpHabitant= attrId;
	}
	
	AttributeHeader ah(tmpArea[0]);
	if (ah.isNULL())
        {
	  TprogramCounter ++;
	}
        else
        {
	  TprogramCounter= brancher(theInstruction, TprogramCounter);
	}
	break;
      }
	
      case Interpreter::EXIT_OK:
	jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(" - exit_ok");
#endif
	return TdataWritten;

      case Interpreter::EXIT_OK_LAST:
	jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(" - exit_ok_last");
#endif
	req_struct->last_row= true;
	return TdataWritten;
	
      case Interpreter::EXIT_REFUSE:
      {
        /**
         * This is a very common exit path, particularly
         * for scans. It simply means that the row didn't
         * fulfil the search condition.
         */
	jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(" - exit_nok");
#endif
	terrorCode = theInstruction >> 16;
        tupkeyErrorLab(req_struct);
        return -1;
      }
      case Interpreter::CALL:
	jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(" - call addr=%u, subroutine len=%u ret addr=%u",
                            theInstruction >> 16, TsubroutineLen,
                            TprogramCounter);
#endif
	RstackPtr++;
	if (RstackPtr < 32)
        {
          TstackMemBuffer[RstackPtr]= TprogramCounter;
          TprogramCounter= theInstruction >> 16;
	  if (TprogramCounter < TsubroutineLen)
          {
	    TcurrentProgram= subroutineProg;
	    TcurrentSize= TsubroutineLen;
	  }
          else
          {
	    return TUPKEY_abort(req_struct, ZCALL_ERROR);
	  }
	}
        else
        {
	  return TUPKEY_abort(req_struct, ZSTACK_OVERFLOW_ERROR);
	}
	break;

      case Interpreter::RETURN:
	jamDebug();
#ifdef TRACE_INTERPRETER
        g_eventLogger->info(" - return to %u from stack level %u",
                            TstackMemBuffer[RstackPtr], RstackPtr);
#endif
	if (RstackPtr > 0)
        {
	  TprogramCounter= TstackMemBuffer[RstackPtr];
	  RstackPtr--;
	  if (RstackPtr == 0)
          {
	    jamDebug();
	    /* ------------------------------------------------------------- */
	    // We are back to the main program.
	    /* ------------------------------------------------------------- */
	    TcurrentProgram= mainProgram;
	    TcurrentSize= TmainProgLen;
	  }
	}
        else
        {
	  return TUPKEY_abort(req_struct, ZSTACK_UNDERFLOW_ERROR);
	}
	break;

      default:
	return TUPKEY_abort(req_struct, ZNO_INSTRUCTION_ERROR);
      }
    }
    else
    {
      return TUPKEY_abort(req_struct, ZOUTSIDE_OF_PROGRAM_ERROR);
    }
  }
  return TUPKEY_abort(req_struct, ZTOO_MANY_INSTRUCTIONS_ERROR);
}

/**
 * expand_var_part - copy packed variable attributes to fully expanded size
 * 
 * dst:        where to start writing attribute data
 * dst_off_ptr where to write attribute offsets
 * src         pointer to packed attributes
 * tabDesc     array of attribute descriptors (used for getting max size)
 * order       Pointer to variable indicating which attributeId this is
 * num_vars    no of atributes to expand
 */
static
Uint32*
expand_var_part(Dbtup::KeyReqStruct::Var_data *dst, 
                const Uint32* src, 
                const Uint32 * tabDesc, 
                const Uint16* order,
                EmulatedJamBuffer *jamBuf)
{
  char* dst_ptr= dst->m_data_ptr;
  Uint32 num_vars = dst->m_var_len_offset;
  Uint16* dst_off_ptr= dst->m_offset_array_ptr;
  Uint16* dst_len_ptr= dst_off_ptr + num_vars;
  const Uint16* src_off_ptr= (const Uint16*)src;
  const char* src_ptr= (const char*)(src_off_ptr + num_vars + 1);

  Uint16 tmp= *src_off_ptr++, next_pos, len, max_len, dst_off= 0;
  for(Uint32 i = 0; i < num_vars; i++)
  {
    next_pos= *src_off_ptr++;
    len= next_pos - tmp;
    require(next_pos >= tmp);

    *dst_off_ptr++ = dst_off; 
    *dst_len_ptr++ = dst_off + len;
    memcpy(dst_ptr, src_ptr, len);
    src_ptr += len;

    max_len= AttributeDescriptor::getSizeInBytes(tabDesc[* order++]);
    thrjamDebug(jamBuf);
    thrjamDataDebug(jamBuf, max_len);
    dst_ptr += max_len; // Max size
    dst_off += max_len;

    tmp= next_pos;
  }
  return ALIGN_WORD(dst_ptr);
}

void
Dbtup::expand_tuple(KeyReqStruct* req_struct, 
                    Uint32 sizes[2],
                    Tuple_header* src, 
                    const Tablerec* tabPtrP,
                    bool disk,
                    bool from_lcp_keep)
{
  /**
   * The source tuple only touches the header parts. The updates of the
   * tuple is applied on the new copy tuple. We still need to ensure that
   * the checksum is correct on the tuple even after changing the header
   * parts since the header is part of the checksum. This is not covered
   * by setting checksum normally since mostly we don't touch the
   * original tuple.
   *
   * This updates the checksum of the source row which has already been
   * made available to the readers. Thus we need to ensure that this
   * write is protected.
   *
   * This updateChecksum seems to always be a NULL op.
   * Verified with ndbrequire
   * updateChecksum(src, tabPtrP, bits, src->m_header_bits);
   */
  Uint32 fix_size= tabPtrP->m_offsets[MM].m_fix_header_size;
  const Uint16 *order = tabPtrP->m_real_order_descriptor;
  Uint32 bits = src->m_header_bits;
  Tuple_header* ptr = req_struct->m_tuple_ptr;
  Uint32 *dst_ptr = ptr->get_end_of_fix_part_ptr(tabPtrP);

  order += tabPtrP->m_attributes[MM].m_no_of_fixsize;
  jamDebug();
  jamDataDebug(tabPtrP->m_attributes[MM].m_no_of_fixsize);
  const Uint32 *src_ptr= src->get_end_of_fix_part_ptr(tabPtrP);
  req_struct->is_expanded= true;

  // Copy in-memory fixed part
  memcpy(ptr, src, 4*fix_size);
  sizes[MM]= 0;
  sizes[DD]= 0;
  Uint32 step = 0; // in bytes

  for (Uint32 ind = 0; ind < 2; ind++)
  {
    Uint32 flex_len = 0;
    const Uint32 *flex_data = nullptr;
    Uint16 num_vars = tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns = tabPtrP->m_attributes[ind].m_no_of_dynamic;
    KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];
    if (num_vars || num_dyns)
    {
      jamDebug();
      /*
       * Reserve place for initial length word and offset array (with one extra
       * offset). This will be filled-in in later, in shrink_tuple().
       */
      dst_ptr += Varpart_copy::SZ32;
    }
    if (ind == DD)
    {
      if (disk == false || tabPtrP->m_no_of_disk_attributes == 0)
      {
        jamDebug();
        ptr->m_header_bits= (bits | Tuple_header::COPY_TUPLE);
        return;
      }
      jamDebug();
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      jamDataDebug(disk_fix_header_size);
      jamDataDebug(tabPtrP->m_attributes[DD].m_no_of_fixsize);
      order += tabPtrP->m_attributes[DD].m_no_of_fixsize;
      Uint32 src_len = disk_fix_header_size;
      if (bits & Tuple_header::DISK_INLINE)
      {
        // Only on copy tuple
        jamDebug();
        ndbassert(bits & Tuple_header::COPY_TUPLE);
        /**
         * Need to set pointer to disk page as preparation for size
         * changes that might occur. In this case we need to check
         * free and used of the disk page to see if we need to select
         * a new disk page.
         */
        req_struct->m_disk_page_ptr.p =
          (Page*)m_global_page_pool.getPtr(req_struct->m_disk_page_ptr.i);
      }
      else
      {
        Local_key key;
        jamDebug();
        /**
         * Can still be a copy tuple if only updates so far without
         * updates of disk columns.
         */
        const Uint32 *disk_ref= src->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        key.m_page_no= req_struct->m_disk_page_ptr.i;
        ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
        src_ptr= get_dd_info(&req_struct->m_disk_page_ptr,
            key,
            tabPtrP,
            src_len);
        jamDataDebug(key.m_page_idx);
        jamDataDebug(src_len);
        DEB_DISK(("(%u) disk_row(%u,%u), src_ptr: %p, src_len: %u",
              instance(),
              key.m_page_no,
              key.m_page_idx,
              src_ptr,
              src_len));
      }

      // Fix diskpart
      req_struct->m_disk_ptr = (Tuple_header*)dst_ptr;
      memcpy(dst_ptr, src_ptr, 4*disk_fix_header_size);
      sizes[DD] = src_len;
      src_ptr += disk_fix_header_size;
      dst_ptr += disk_fix_header_size;
      if (bits & Tuple_header::DISK_VAR_PART)
      {
        jamDebug();
        ndbrequire(tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData);
        if ((num_vars + num_dyns) > 0)
        {
          if (! (bits & Tuple_header::DISK_INLINE))
          {
            jamDebug();
            PagePtr pagePtr;
            flex_len = src_len - disk_fix_header_size;
            flex_data = src_ptr;
            req_struct->m_varpart_page_ptr[DD] = req_struct->m_disk_page_ptr;
          }
          else
          {
            jamDebug();
            Varpart_copy* vp = (Varpart_copy*)src_ptr;
            flex_len = vp->m_len;
            flex_data = vp->m_data;
            req_struct->m_varpart_page_ptr[DD] = req_struct->m_page_ptr;
            sizes[DD] += flex_len;
          }
        }
      }
      else
      {
        bool var_part = (tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData);
        ndbrequire(!var_part);
      }
      if (unlikely(req_struct->m_disk_ptr->m_base_record_page_idx >=
            Tup_page::DATA_WORDS))
      {
        Local_key key;
        const Uint32 *disk_ref= src->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        g_eventLogger->info("(%u) Crash on error in disk ref on row(%u,%u)"
            ", disk_page(%u,%u).%u, disk_page_ptr.i = %u"
            ", size: %u, disk_ptr: %p",
            instance(),
            req_struct->frag_page_id,
            req_struct->operPtrP->m_tuple_location.m_page_idx,
            key.m_file_no,
            key.m_page_no,
            key.m_page_idx,
            req_struct->m_disk_page_ptr.i,
            req_struct->m_disk_ptr->m_base_record_page_idx,
            req_struct->m_disk_ptr);
        ndbrequire(req_struct->m_disk_ptr->m_base_record_page_idx <
            Tup_page::DATA_WORDS);
      }
    }
    else
    {
      if (bits & Tuple_header::VAR_PART)
      {
        jamDebug();
        if (! (bits & Tuple_header::COPY_TUPLE))
        {
          jamDebug();
          /* This is for the initial expansion of a stored row. */
          const Var_part_ref* var_ref = src->get_var_part_ref_ptr(tabPtrP);
          Ptr<Page> var_page;
          flex_data= get_ptr(&var_page, *var_ref);
          flex_len= get_len(&var_page, *var_ref);
          jam();
          /**
           * Coming here with MM_GROWN set is possible if we are coming here
           * from handle_lcp_keep_commit. In this case we are currently
           * performing a DELETE operation. This operation is the final
           * operation that will be committed. It could very well have
           * been preceeded by an UPDATE operation that did set the
           * MM_GROWN bit. In this case it is important to get the original
           * length from the end of the varsize part and not the page
           * entry length which is essentially the meaning of the MM_GROWN
           * bit.
           *
           * An original tuple can't have grown as we're expanding it...
           * else we would be "re-expanding". This is the case when coming
           * here as part of INSERT/UPDATE/REFRESH. We assert on that we
           * don't do any "re-expanding".
           */
          if (bits & Tuple_header::MM_GROWN)
          {
            jam();
            ndbrequire(from_lcp_keep);
            ndbassert(flex_len>0);
            flex_len= flex_data[flex_len-1];
          }
          sizes[MM]= flex_len;
          step= 0;
          req_struct->m_varpart_page_ptr[MM] = var_page;
        }
        else
        {
          /* This is for the re-expansion of a shrunken row (update2 ...) */
          Varpart_copy* vp = (Varpart_copy*)src_ptr;
          flex_len = vp->m_len;
          flex_data= vp->m_data;
          step = (Varpart_copy::SZ32 + flex_len); // 1+ is for extra word
          req_struct->m_varpart_page_ptr[MM] = req_struct->m_page_ptr;
          sizes[MM]= flex_len;
          jamDebug();
          jamDataDebug(flex_len);
        }
      }
    }
    Uint32 dyn_len = flex_len;
    const Uint32 *dyn_data = flex_data;
    if (num_vars)
    {
      jamDebug();
      ndbrequire(flex_data != nullptr);
      const Uint32 *desc = req_struct->attr_descr;
      dst->m_data_ptr= (char*)(((Uint16*)dst_ptr)+num_vars+1);
      dst->m_offset_array_ptr= req_struct->var_pos_array[ind];
      dst->m_var_len_offset= num_vars;
      dst->m_max_var_offset= tabPtrP->m_offsets[ind].m_max_var_offset;

      dst_ptr= expand_var_part(dst, flex_data, desc, order, jamBuffer());
      order += num_vars;
      ndbassert(dst_ptr == ALIGN_WORD(dst->m_data_ptr + dst->m_max_var_offset));
      /**
       * Move to end of fix varpart
       */
      char* varstart = (char*)(((Uint16*)flex_data)+num_vars+1);
      Uint32 varlen = ((Uint16*)flex_data)[num_vars];
      Uint32 *dynstart = ALIGN_WORD(varstart + varlen);

      ndbassert((ptrdiff_t)flex_len >= (dynstart - flex_data));
      dyn_len -= Uint32(dynstart - flex_data);
      dyn_data = dynstart;
    }
    if (num_dyns)
    {
      jamDebug();
      Uint16 num_dynfix= tabPtrP->m_attributes[ind].m_no_of_dyn_fix;
      Uint16 num_dynvar= tabPtrP->m_attributes[ind].m_no_of_dyn_var;
      const Uint32 *desc = req_struct->attr_descr;
      /**
       * dynattr needs to be expanded even if no varpart existed before
       */
      dst->m_dyn_offset_arr_ptr= req_struct->var_pos_array[ind]+2*num_vars;
      dst->m_dyn_len_offset= num_dynvar+num_dynfix;
      dst->m_max_dyn_offset= tabPtrP->m_offsets[ind].m_max_dyn_offset;
      dst->m_dyn_data_ptr= (char*)dst_ptr;
      dst_ptr= expand_dyn_part(dst,
                               dyn_data,
                               dyn_len,
                               desc,
                               order,
                               num_dynvar, num_dynfix,
                               tabPtrP->m_offsets[ind].m_dyn_null_words);
      order += (num_dynvar + num_dynfix);
    }
    
    ndbassert((UintPtr(src_ptr) & 3) == 0);
    src_ptr = src_ptr + step;
  }
  ptr->m_header_bits = (bits |
                        Tuple_header::COPY_TUPLE |
                        Tuple_header::DISK_INLINE);
}

void
Dbtup::dump_tuple(const KeyReqStruct* req_struct, const Tablerec* tabPtrP)
{
  Uint16 mm_vars= tabPtrP->m_attributes[MM].m_no_of_varsize;
  Uint16 mm_dyns= tabPtrP->m_attributes[MM].m_no_of_dynamic;
  //Uint16 dd_tot= tabPtrP->m_no_of_disk_attributes;
  const Tuple_header* ptr= req_struct->m_tuple_ptr;
  Uint32 bits= ptr->m_header_bits;
  const Uint32 *tuple_words= (Uint32 *)ptr;
  const Uint32 *fix_p;
  Uint32 fix_len;
  const Uint32 *var_p;
  Uint32 var_len;
  //const Uint32 *disk_p;
  //Uint32 disk_len;
  const char *typ;

  fix_p= tuple_words;
  fix_len= tabPtrP->m_offsets[MM].m_fix_header_size;
  if(req_struct->is_expanded)
  {
    typ= "expanded";
    var_p= ptr->get_end_of_fix_part_ptr(tabPtrP);
    var_len= 0;                                 // No dump of varpart in expanded
#if 0
    disk_p= (Uint32 *)req_struct->m_disk_ptr;
    disk_len= (dd_tot ? tabPtrP->m_offsets[DD].m_fix_header_size : 0);
#endif
  }
  else if(! (bits & Tuple_header::COPY_TUPLE))
  {
    typ= "stored";
    if(mm_vars+mm_dyns)
    {
      //const KeyReqStruct::Var_data* dst= &req_struct->m_var_data[MM];
      const Var_part_ref *varref= ptr->get_var_part_ref_ptr(tabPtrP);
      Ptr<Page> tmp;
      var_p= get_ptr(&tmp, * varref);
      var_len= get_len(&tmp, * varref);
    }
    else
    {
      var_p= 0;
      var_len= 0;
    }
#if 0
    if(dd_tot)
    {
      Local_key key;
      memcpy(&key, ptr->get_disk_ref_ptr(tabPtrP), sizeof(key));
      key.m_page_no= req_struct->m_disk_page_ptr.i;
      disk_p= get_dd_len(&req_struct->m_disk_page_ptr,
                         &key,
                         tabPtrP,
                         &disk_len);
    }
    else
    {
      disk_p= var_p;
      disk_len= 0;
    }
#endif
  }
  else
  {
    typ= "shrunken";
    if(mm_vars+mm_dyns)
    {
      var_p= ptr->get_end_of_fix_part_ptr(tabPtrP);
      var_len= *((Uint16 *)var_p) + 1;
    }
    else
    {
      var_p= 0;
      var_len= 0;
    }
#if 0
    disk_p= (Uint32 *)(req_struct->m_disk_ptr);
    disk_len= (dd_tot ? tabPtrP->m_offsets[DD].m_fix_header_size : 0);
#endif
  }
  g_eventLogger->info("Fixed part[%s](%p len=%u words)", typ, fix_p, fix_len);
  dump_hex(fix_p, fix_len);
  g_eventLogger->info("Varpart part[%s](%p len=%u words)", typ, var_p, var_len);
  dump_hex(var_p, var_len);
#if 0
  g_eventLogger->info("Disk part[%s](%p len=%u words)", typ, disk_p, disk_len);
  dump_hex(disk_p, disk_len);
#endif
}

void
Dbtup::prepare_read(KeyReqStruct* req_struct, 
		    Tablerec* tabPtrP,
                    bool disk)
{
  Tuple_header* ptr = req_struct->m_tuple_ptr;
  Uint32 bits = ptr->m_header_bits;
  const Uint32 *src_ptr = ptr->get_end_of_fix_part_ptr(tabPtrP);
  req_struct->is_expanded= false;

  /**
   * We can have 0 varsized columns and a number of dynamic columns
   * that are all set to NULL values. In this case we can arrive
   * here and still have no var part. The flag VAR_PART indicates
   * there is an in-memory var part and DISK_VAR_PART indicates there
   * is a var part in the disk part.
   *
   * We also make use of the fact that if VAR_PART is set on a tuple
   * then definitely there is either a varsized or dynamic in-memory
   * column and similarly for the disk part.
   */
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    KeyReqStruct::Var_data* dst= &req_struct->m_var_data[ind];
    Uint16 num_vars= tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns= tabPtrP->m_attributes[ind].m_no_of_dynamic;
    /**
     * Pointer to and length of the dynamic part of the row, this
     * consists of the variable sized columns with fixed length
     * parts and the dynamic parts. We call the variable flex*
     * since they are the flexible part of the row.
     */
    Uint32 flex_len = 0;
    const Uint32 *flex_data = nullptr;

    if (ind == DD)
    {
      req_struct->m_disk_ptr = nullptr;
      if ((disk == false) || (tabPtrP->m_no_of_disk_attributes == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        return;
      }
      req_struct->m_disk_ptr = (Tuple_header*)src_ptr;
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      if (! (bits & Tuple_header::DISK_INLINE))
      {
        thrjam(req_struct->jamBuffer);
        /**
         * We will read the disk part row from the disk page, no previous
         * updates of the disk columns have occurred in this transaction
         * so far. This means that for these reads we could fetch the
         * in-memory parts from the Copy row and the disk parts from
         * the disk page.
         */
        Local_key key;
        const Uint32 *disk_ref = ptr->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        key.m_page_no = req_struct->m_disk_page_ptr.i;
        ndbrequire(key.m_page_idx < Tup_page::DATA_WORDS);
        Uint32 disk_len = 0;
        src_ptr = get_dd_info(&req_struct->m_disk_page_ptr,
                              key,
                              tabPtrP,
                              disk_len);
        req_struct->m_disk_ptr = (Tuple_header*)src_ptr;
        flex_data = src_ptr + disk_fix_header_size;
        /**
         * Move past the fixed size columns to set src_ptr to point to
         * where the varsized columns start.
         */
        ndbrequire(disk_len >= disk_fix_header_size);
        flex_len = disk_len - disk_fix_header_size;
      }
      else
      {
        thrjam(req_struct->jamBuffer);
        /**
         * On COPY tuples the disk data columns comes immediately after
         * the in-memory columns. The address was calculated in the first
         * loop and thus src_ptr already points to the first set of disk
         * data columns.
         */
        ndbrequire(bits & Tuple_header::COPY_TUPLE);
        src_ptr += disk_fix_header_size;
        if (bits & Tuple_header::DISK_VAR_PART)
        {
          thrjam(req_struct->jamBuffer);
          Varpart_copy* vp = (Varpart_copy*)src_ptr;
          flex_len = vp->m_len;
          flex_data = vp->m_data;
          src_ptr++;
        }
      }
      if (unlikely(req_struct->m_disk_ptr->m_base_record_page_idx >=
                   Tup_page::DATA_WORDS))
      {
        Local_key key;
        const Uint32 *disk_ref = ptr->get_disk_ref_ptr(tabPtrP);
        memcpy(&key, disk_ref, sizeof(key));
        g_eventLogger->info(
          "Crash: page(%u,%u,%u,%u).%u, DISK_INLINE= %u, tab(%x,%x,%x)"
                       ", frag_page_id:%u, rowid_ref(%u,%u)",
                       instance(),
                       req_struct->m_disk_page_ptr.i,
                       req_struct->m_disk_page_ptr.p->m_file_no,
                       req_struct->m_disk_page_ptr.p->m_page_no,
                       key.m_page_idx,
                       bits & Tuple_header::DISK_INLINE ? 1 : 0,
                       req_struct->m_disk_page_ptr.p->m_table_id,
                       req_struct->m_disk_page_ptr.p->m_fragment_id,
                       req_struct->m_disk_page_ptr.p->m_create_table_version,
                       req_struct->frag_page_id,
                       req_struct->m_disk_ptr->m_base_record_page_no,
                       req_struct->m_disk_ptr->m_base_record_page_idx);
        ndbrequire(req_struct->m_disk_ptr->m_base_record_page_idx <
                   Tup_page::DATA_WORDS);
      }
      if (unlikely((bits & Tuple_header::DISK_VAR_PART) == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        ndbrequire((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0);
        dst->m_max_var_offset = 0;
        dst->m_dyn_part_len = 0;
#if defined(VM_TRACE) || defined(ERROR_INSERT)
        std::memset(dst, 0, sizeof(* dst));
#endif
        return;
      }
    }
    else
    {
      /* ind == MM */
      if (num_vars == 0 && num_dyns == 0)
      {
        thrjamDebug(req_struct->jamBuffer);
        continue;
      }
      if (unlikely((bits & Tuple_header::VAR_PART) == 0))
      {
        thrjamDebug(req_struct->jamBuffer);
        dst->m_max_var_offset = 0;
        dst->m_dyn_part_len = 0;
#if defined(VM_TRACE) || defined(ERROR_INSERT)
        std::memset(dst, 0, sizeof(* dst));
#endif
        continue;
      }
      if (! (bits & Tuple_header::COPY_TUPLE))
      {
        thrjamDebug(req_struct->jamBuffer);
        Ptr<Page> tmp;
        Var_part_ref* var_ref = ptr->get_var_part_ref_ptr(tabPtrP);
        flex_data= get_ptr(&tmp, * var_ref);
        flex_len= get_len(&tmp, * var_ref);

        /* If the original tuple was grown,
         * the old size is stored at the end. */
        if (bits & Tuple_header::MM_GROWN)
        {
          /**
           * This is when triggers read before value of update
           *   when original has been reallocated due to grow
           */
          ndbassert(flex_len>0);
          thrjam(req_struct->jamBuffer);
          flex_len= flex_data[flex_len-1];
        }
      }
      else
      {
        thrjam(req_struct->jamBuffer); // Read Copy tuple
        Varpart_copy* vp = (Varpart_copy*)src_ptr;
        flex_len = vp->m_len;
        flex_data = vp->m_data;
        src_ptr++;
      }
      /* Set up src_ptr for DD loop */
      src_ptr += flex_len;
    }
    char* varstart;
    Uint32 varlen;
    const Uint32* dynstart;
    if (num_vars)
    {
      varstart = (char*)(((Uint16*)flex_data)+ num_vars + 1);
      varlen = ((Uint16*)flex_data)[num_vars];
      dynstart = ALIGN_WORD(varstart + varlen);
#ifdef TUP_DATA_VALIDATION
      thrjam(req_struct->jamBuffer);
      thrjamLine(req_struct->jamBuffer, num_vars);
      for (Uint16 i = 0; i < (num_vars + 1); i++)
        thrjamLine(req_struct->jamBuffer, ((Uint16*)flex_data)[i]);
#endif
    }
    else
    {
#ifdef TUP_DATA_VALIDATION
      thrjam(req_struct->jamBuffer);
#endif
      varstart = 0;
      varlen = 0;
      dynstart = flex_data;
    }

    dst->m_data_ptr= varstart;
    dst->m_offset_array_ptr = (Uint16*)flex_data;
    dst->m_var_len_offset = 1;
    dst->m_max_var_offset = varlen;

    Uint32 dynlen = Uint32(flex_len - (dynstart - flex_data));
    ndbassert((ptrdiff_t)flex_len >= (dynstart - flex_data));
    dst->m_dyn_data_ptr= (char*)dynstart;
    dst->m_dyn_part_len= dynlen;
  }
}

void
Dbtup::shrink_tuple(KeyReqStruct* req_struct, Uint32 sizes[2],
		    const Tablerec* tabPtrP, bool disk)
{
  ndbassert(tabPtrP->need_shrink());
  ndbassert(req_struct->is_expanded);
  Tuple_header* ptr= req_struct->m_tuple_ptr;
  ndbassert(ptr->m_header_bits & Tuple_header::COPY_TUPLE);
  
  const Uint16* order= tabPtrP->m_real_order_descriptor;
  const Uint32 * tabDesc = req_struct->attr_descr;
  
  Uint32 *dst_ptr= ptr->get_end_of_fix_part_ptr(tabPtrP);

  /**
   * shrink_tuple is called when there is disk attributes and/or
   * when there is a variable sized in-memory part. Thus we could
   * come here without a variable sized part.
   */
  sizes[MM] = 0;
  sizes[DD] = 0;

  /**
   * No need to copy the fixed size memory parts, those are
   * already in the correct position.
   */
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    Uint16 num_fix= tabPtrP->m_attributes[ind].m_no_of_fixsize;
    Uint16 num_vars= tabPtrP->m_attributes[ind].m_no_of_varsize;
    Uint16 num_dyns= tabPtrP->m_attributes[ind].m_no_of_dynamic;
    if (ind == DD)
    {
      Uint16 dd_tot = tabPtrP->m_no_of_disk_attributes;
      if (!(disk && dd_tot))
      {
        jamDebug();
        break;
      }
      Uint32 * src_ptr = (Uint32*)req_struct->m_disk_ptr;
      req_struct->m_disk_ptr = (Tuple_header*)dst_ptr;
      Uint32 disk_fix_header_size = tabPtrP->m_offsets[DD].m_fix_header_size;
      sizes[DD] = disk_fix_header_size;
      memmove(dst_ptr, src_ptr, 4 * disk_fix_header_size);
      dst_ptr += disk_fix_header_size;
      if ((tabPtrP->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
      {
        jamDebug();
        ptr->m_header_bits |= Tuple_header::DISK_VAR_PART;
      }
      else
      {
        jamDebug();
      }
    }
    order += num_fix;
    if (num_vars || num_dyns)
    {
      jamDebug();
      Varpart_copy* vp = (Varpart_copy*)dst_ptr;
      Uint32* varstart = vp->m_data;
      dst_ptr = vp->m_data;

      if (num_vars)
      {
        jamDebug();
        Uint16* src_off_ptr= req_struct->var_pos_array[ind];
        Uint16* dst_off_ptr= (Uint16*)dst_ptr;
        char*  dst_data_ptr= (char*)(dst_off_ptr + num_vars + 1);
        char* src_data_ptr = (char*)req_struct->m_var_data[ind].m_data_ptr;
        ndbassert((ind == DD) || (src_data_ptr == dst_data_ptr));
        Uint32 off= 0;
        for (Uint32 i = 0; i < num_vars; i++)
        {
          /**
           * var_pos_array has one index for main memory part and
           * the second is the disk columns part.
           *
           * Each var_pos_array has 2 parts, the first is index by the
           * index of the varsize column, the second is the index
           * plus the number of varsize columns. These were initialised
           * to the position of the start of the column (both of them),
           * starting at position 0.
           *
           * When updating the varsize column we set the index plus
           * num_vars to instead be position of the end of the varsize
           * column. Thus var_pos_array[num_vars + i] - var_pos_array[i]
           * is the length of the column. For NULL values both are set
           * to the position of the column and thus length is 0.
           *
           * Seems a bit complicated manner to calculate length, but it
           * means that we retain the position of the column.
           *
           * In the stored row we store the offset of each varsize column,
           * starting at 0, in addition we store the total length of all
           * varsize column as an extra length information.
           */
          const char* data_ptr= src_data_ptr + *src_off_ptr;
          Uint32 len= src_off_ptr[num_vars] - *src_off_ptr;
          * dst_off_ptr++= off;
          memmove(dst_data_ptr, data_ptr, len);
          off += len;
          src_off_ptr++;
          dst_data_ptr += len;
          jamDebug();
          jamDataDebug(len);
        }
        *dst_off_ptr= off;
        dst_ptr = ALIGN_WORD(dst_data_ptr);
        order += num_vars; // Point to first dynfix entry
      }

      if (num_dyns)
      {
        jamDebug();
        Uint16 num_dynvar= tabPtrP->m_attributes[ind].m_no_of_dyn_var;
        Uint16 num_dynfix= tabPtrP->m_attributes[ind].m_no_of_dyn_fix;
        KeyReqStruct::Var_data* dst = &req_struct->m_var_data[ind];
        dst_ptr = shrink_dyn_part(dst,
                                  dst_ptr,
                                  tabPtrP,
                                  tabDesc,
                                  order,
                                  num_dynvar,
                                  num_dynfix,
                                  ind);
        order += (num_dynfix + num_dynvar);
      }
      Uint32 varpart_len_words = Uint32(dst_ptr - varstart);
      ndbassert(varpart_len_words <= MAX_EXPANDED_TUPLE_SIZE_IN_WORDS);
      vp->m_len = varpart_len_words;
      if (ind == MM)
      {
        sizes[MM] = varpart_len_words;
        if (varpart_len_words != 0)
        {
          jamDebug();
          jamDataDebug(varpart_len_words);
          ptr->m_header_bits |= Tuple_header::VAR_PART;
        }
        else if ((ptr->m_header_bits & Tuple_header::VAR_PART) == 0)
        {
          jamDebug();
          /*
           * No varpart present.
           * And this is not an update where the dynamic column is set to null.
           * So skip storing the var part altogether.
           */
          ndbassert(((Uint32*) vp) == ptr->get_end_of_fix_part_ptr(tabPtrP));
          dst_ptr= (Uint32*)vp;
        }
        else
        {
          jamDebug();
          /*
           * varpart_len is now 0, but tuple already had a varpart.
           * It will be released at commit time.
           */
        }
      }
      else
      {
        sizes[DD] += varpart_len_words;
        jamDebug();
        jamDataDebug(varpart_len_words);
      }
      ndbassert((UintPtr(ptr) & 3) == 0);
      ndbassert(varpart_len_words < 0x2000);
    }
  }
  req_struct->is_expanded= false;
}

void
Dbtup::validate_page(TablerecPtr regTabPtr, Var_page* p)
{
  /* ToDo: We could also do some checks here for any dynamic part. */
  Uint32 mm_vars= regTabPtr.p->m_attributes[MM].m_no_of_varsize;
  Uint32 fix_sz= regTabPtr.p->m_offsets[MM].m_fix_header_size + 
    Tuple_header::HeaderSize;
    
  if(mm_vars == 0)
    return;
  
  for(Uint32 F= 0; F < MAX_FRAG_PER_LQH; F++)
  {
    FragrecordPtr fragPtr;

    fragPtr.i = c_lqh->m_ldm_instance_used->getNextTupFragrec(regTabPtr.i, F);
    if (fragPtr.i == RNIL64)
      continue;

    ndbrequire(c_fragment_pool.getPtr(fragPtr));
    for(Uint32 P= 0; P<fragPtr.p->noOfPages; P++)
    {
      Uint32 real= getRealpid(fragPtr.p, P);
      Var_page* page= (Var_page*)c_page_pool.getPtr(real);

      for(Uint32 i=1; i<page->high_index; i++)
      {
	Uint32 idx= page->get_index_word(i);
	Uint32 len = (idx & Var_page::LEN_MASK) >> Var_page::LEN_SHIFT;
	if(!(idx & Var_page::FREE) && !(idx & Var_page::CHAIN))
	{
	  Tuple_header *ptr= (Tuple_header*)page->get_ptr(i);
	  Uint32 *part= ptr->get_end_of_fix_part_ptr(regTabPtr.p);
	  if(! (ptr->m_header_bits & Tuple_header::COPY_TUPLE))
	  {
	    ndbrequire(len == fix_sz + 1);
            Local_key tmp;
            Var_part_ref* vpart = reinterpret_cast<Var_part_ref*>(part);
            vpart->copyout(&tmp);
#if defined(VM_TRACE) || defined(ERROR_INSERT)
            ndbrequire(!"Looking for test coverage - found it!");
#endif
	    Ptr<Page> tmpPage;
	    part= get_ptr(&tmpPage, *vpart);
	    len= ((Var_page*)tmpPage.p)->get_entry_len(tmp.m_page_idx);
	    Uint32 sz= ((mm_vars + 1) << 1) + (((Uint16*)part)[mm_vars]);
	    ndbrequire(len >= ((sz + 3) >> 2));
	  } 
	  else
	  {
	    Uint32 sz= ((mm_vars + 1) << 1) + (((Uint16*)part)[mm_vars]);
	    ndbrequire(len >= ((sz+3)>>2)+fix_sz);
	  }
	  if(ptr->m_operation_ptr_i != RNIL)
	  {
            OperationrecPtr operPtr;
            operPtr.i = ptr->m_operation_ptr_i;
	    ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(operPtr));
	  }
	} 
	else if(!(idx & Var_page::FREE))
	{
	  /**
	   * Chain
	   */
	  Uint32 *part= page->get_ptr(i);
	  Uint32 sz= ((mm_vars + 1) << 1) + (((Uint16*)part)[mm_vars]);
	  ndbrequire(len >= ((sz + 3) >> 2));
	} 
	else 
	{
	  
	}
      }
      if(p == 0 && page->high_index > 1)
	page->reorg((Var_page*)ctemp_page);
    }
  }
  
  if(p == 0)
  {
    validate_page(regTabPtr, (Var_page*)1);
  }
}

int
Dbtup::handle_size_change_after_update(Signal *signal,
                                       KeyReqStruct* req_struct,
				       Tuple_header* org,
				       Operationrec* regOperPtr,
				       Fragrecord* regFragPtr,
				       Tablerec* regTabPtr,
				       Uint32 sizes[4])
{
  Uint32 bits = m_base_header_bits;
  Uint32 copy_bits= req_struct->m_tuple_ptr->m_header_bits;
  
  DEB_LCP(("(%u)size_change: tab(%u,%u), row_id(%u,%u), old: %u, new: %u",
          instance(),
          req_struct->fragPtrP->fragTableId,
          req_struct->fragPtrP->fragmentId,
          regOperPtr->m_tuple_location.m_page_no,
          regOperPtr->m_tuple_location.m_page_idx,
          sizes[2+MM],
          sizes[MM]));
  for (Uint32 ind = 0; ind < 2; ind++)
  {
    jamDebug();
    jamDataDebug(ind);
    if(sizes[2+ind] == sizes[ind])
    {
      jam();
      continue;
    }
    else if(sizes[2+ind] < sizes[ind])
    {
      jam();
      continue;
    }
    jam();
    if (ind == DD)
    {
      jamDebug();
      /**
       * We have updated the disk part row such that it has grown, this
       * means that we need to ensure that more space is allocated before
       * can proceed, we will attempt to use the current disk page. But if
       * this lacks space for the new size we will allocate space in
       * another page.
       *
       * We use the No Steal approach which means that we are not allow
       * to write dirty information to a page, thus we cannot touch the
       * disk page until we commit it. Thus we cannot follow the same
       * approach as for variable sized memory pages. We have to allocate
       * a new area for the row and ensure that there is space for the new
       * disk part row should the transaction finally commit.
       *
       * We need however to write some temporary information to the page.
       * This information doesn't affect any of the row data in the page.
       * It only affects the page header information. The variables we
       * can change here are:
       * 1) uncommitted_used_space
       *    We need this variable to preallocate area on the page.
       *    This variable is changed only when we increase the row size
       *    on the same page or dropping a row due to moving the row
       *    to a new page.
       * 2) m_restart_seq
       *    The number of the restart that the page was last written.
       *
       * This variable is affected by calls to disk_page_prealloc, this call
       * use the ALLOC_REQ flag to PGMAN, this will make the page dirty and
       * thus no special handling of this flag is required.
       *
       * When we read the page before an UPDATE/DELETE/WRITE we haven't
       * set anything that indicates that the page is dirty. If we update
       * uncommitted_used_space we thus need to indicate to PGMAN that
       * the page must be written to disk before cleaned out. We do this
       * through a get_page call using the DIRTY_HEADER flag.
       *
       * The free_space variable is read, but this is only updated during
       * commit of a transaction.
       */
      if ((regTabPtr->m_bits & Tablerec::TR_UseVarSizedDiskData) != 0)
      {
        PagePtr diskPagePtr = req_struct->m_disk_page_ptr;
        Uint32 free = diskPagePtr.p->free_space;
        Uint32 used = diskPagePtr.p->uncommitted_used_space;
        Uint32 new_size = sizes[2+DD];
        bool disk_alloc_flag = copy_bits & Tuple_header::DISK_ALLOC;
        bool disk_reorg_flag = bits & Tuple_header::DISK_REORG;
        DEB_REORG(("(%u) free: %u, used: %u, new_size: %u, size[DD]: %u"
                   ", alloc_flag: %u, reorg_flag: %u",
                   instance(),
                   free,
                   used,
                   new_size,
                   sizes[DD],
                   disk_alloc_flag,
                   disk_reorg_flag));
        if (unlikely(disk_alloc_flag || disk_reorg_flag))
        {
          jamDebug();
          ndbrequire(regOperPtr->m_uncommitted_used_space == 0);
          /**
           * disk_alloc_flag true:
           * We started the transaction without a row in this position.
           * Thus this must be a multi-row operation that either
           * re-inserts the row or updates the row with a new size.
           *
           * In this case we have allocated the row in a page which we
           * have access to, thus we can either change the size used by
           * this page or move the row to a new page.
           *
           * disk_reorg_flag true:
           * We have already previously moved to a new disk page.
           * Thus we need to check if the new page is large enough
           * handle also the new size of the row. This is very
           * similar to the handling of a row operation that starts
           * with an initial insert operation.
           *
           * One difference is that the size of the new page is found
           * in the copy row. Another difference is that diskPagePtr
           * refers to the original page and thus using
           * disk_page_abort_prealloc_callback_1 directly is replaced
           * by using disk_page_abort_prealloc. We still know that
           * the page is retrieved before reaching here, so we are
           * safe that no real-time break will happen here.
           */
          Local_key key;
          PagePtr used_pagePtr;
          const Uint32 *disk_ref =
            req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr);
          memcpy(&key, disk_ref, sizeof(key));
          jamDataDebug(key.m_file_no);
          jamDataDebug(key.m_page_no);
          if (disk_reorg_flag)
          {
            jamDebug();
            used_pagePtr.i = regOperPtr->m_disk_extra_callback_page;
            used_pagePtr.p =
              (Page*)m_global_page_pool.getPtr(used_pagePtr.i);
          }
          else
          {
            jamDebug();
            used_pagePtr = diskPagePtr;
          }
          Uint32 curr_size = key.m_page_idx;
          Int32 add = new_size - curr_size;
          jamDataDebug(new_size);
          jamDataDebug(curr_size);
          DEB_REORG(("(%u) (file,page): (%u,%u), new_size: %u, curr_size: %u"
                     ", add: %d, free: %u, used: %u",
                     instance(),
                     key.m_file_no,
                     key.m_page_no,
                     new_size,
                     curr_size,
                     add,
                     free,
                     used));
          if ((used + add) <= free)
          {
            jamDebug();
            /**
             * The row fits in the page already used. We will either
             * add more to the uncommitted_used_space of the page or
             * decrease it.
             */
            if (add > 0)
            {
              jamDebug();
              /**
               * We also need to update the size allocated that is stored
               * in the local key of the disk row reference. When DISK_ALLOC
               * is set (an initial insert is the first operation on the row).
               * There is no need to set it in the disk row reference stored
               * in the in-memory row that will become the row. We set it
               * anyways for consistency.
               *
               * Need to set the checksum using the entire row, this happens
               * with exclusive access, so it is safe to set a new checksum.
              */
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     used_pagePtr,
                                     add);
              key.m_page_idx = new_size;
              memcpy(req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
                     &key,
                     sizeof(key));
              if (disk_alloc_flag)
              {
                jam();
                memcpy(org->get_disk_ref_ptr(regTabPtr),
                       &key,
                       sizeof(key));
                setChecksum(org, regTabPtr);
              }
            }
            else
            {
              jamDebug();
              /**
               * We follow the principle that we never return any preallocated
               * storage. This would create problems if we roll back one or
               * more operations. We cannot safely allocate storage later in
               * the process and we cannot abort already prepared operations.
               * Thus we avoid returning memory here.
               */
            }
          }
          else
          {
            jamDebug();
            ndbrequire(add > 0);
            /**
             * We grew out of the current page, we need to allocate a new page
             * and deallocate the current page.
             * We deallocate after allocating from a new page. Finally
             * we also need to update the disk reference in the copy row.
             */
            /* Add extra word to handle possible directory size increase.*/
            new_size++;
            Local_key new_key;
            jamDebug();
            int ret = disk_page_prealloc(signal,
                                         prepare_fragptr,
                                         regTabPtr,
                                         &new_key,
                                         new_size);
            if (unlikely(ret < 0))
            {
              jam();
              terrorCode = -ret;
              return ret;
            }
            jamDebug();
            jamDataDebug(new_key.m_file_no);
            jamDataDebug(new_key.m_page_no);
            new_key.m_page_idx = new_size;
            memcpy(req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr),
                   &new_key,
                   sizeof(new_key));
            if (disk_alloc_flag)
            {
              jam();
              /**
               * Update the disk row reference also in the row to be
               * committed to ensure that we handle any further row
               * operation on the same row in the same transaction
               * in a proper manner.
               *
               * Ensure that checksum is updated as well, we have exclusive
               * access, so no worries with that.
               */
              memcpy(org->get_disk_ref_ptr(regTabPtr),
                     &new_key,
                     sizeof(new_key));
              setChecksum(org, regTabPtr);
            }
            ndbrequire(used_pagePtr.p->m_restart_seq == globalData.m_restart_seq);
            disk_page_abort_prealloc_callback_1(signal,
                                                regFragPtr,
                                                used_pagePtr,
                                                curr_size,
                                                0);
          }
          return 0;
        }
        else
        {
          jamDebug();
          Local_key key;
          const Uint32 *disk_ref= org->get_disk_ref_ptr(regTabPtr);
          memcpy(&key, disk_ref, sizeof(key));
          Uint32 curr_size =
            ((Var_page*)diskPagePtr.p)->get_entry_len(key.m_page_idx);
          Int32 add = new_size - curr_size;
          add -= regOperPtr->m_uncommitted_used_space;
          jamDataDebug(new_size);
          jamDataDebug(curr_size);
          jamDataDebug(regOperPtr->m_uncommitted_used_space);
          DEB_REORG(("(%u) key(%u,%u,%u), new_size: %u, curr_size: %u"
                     ", add: %d, uncommitted_used_space: %u",
                     instance(),
                     key.m_file_no,
                     key.m_page_no,
                     key.m_page_idx,
                     new_size,
                     curr_size,
                     add,
                     regOperPtr->m_uncommitted_used_space));
          /**
           * curr_size is the size of the row before the transaction
           * started. new_size is the size after this operation is
           * completed. regOperPtr->m_uncommitted_used_space is the
           * size we already seized in the page, thus add will here
           * only be positive if we need to increase our allocation.
           */
          if (Int32(Int32(used) + add) <= Int32(free))
          {
            /**
             * Size of new row fits in the current disk page, no need
             * to move the row to another page.
             */
            if (add > 0)
            {
              jamDebug();
              jamDataDebug(Uint16(add));
              /* Size has grown, but we still fit in the original disk page. */
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     diskPagePtr,
                                     add);
              regOperPtr->m_uncommitted_used_space += add;
              jamDebug();
              jamDataDebug(Uint16(regOperPtr->m_uncommitted_used_space));
            }
            else
            {
              jamDebug();
              /**
               * The row has either shrunk or is of the same size, so no need
               * to do anything since we cannot shrink the original row until
               * commit time.
               *
               * We will never decrease m_uncommitted_used_space to allow for
               * various abort variants. Otherwise we might have to allocate
               * space in abort or commit which is not safe.
               */
            }
          }
          else
          {
            jamDebug();
            /**
             * The row will no longer fit in the original page. Thus we have
             * to move the row to a new page. We set the DISK_REORG flag.
             */
            jam();
            ndbrequire(add > 0);
            Uint32 undo_len = sizeof(Dbtup::Disk_undo::Alloc) >> 2;
            {
              Logfile_client lgman(this,
                                   c_lgman,
                                   regFragPtr->m_logfile_group_id);
              DEB_LCP_LGMAN(("(%u)alloc_log_space(%u): %u",
                             instance(),
                             __LINE__,
                             undo_len));
              terrorCode = lgman.alloc_log_space(
                undo_len,
                true,
                !req_struct->m_nr_copy_or_redo,
                jamBuffer());
            }
            if (unlikely(terrorCode))
            {
              jam();
              return -1;
            }
            jamDebug();
            /**
             * There are two ways to get here. The first is by starting with
             * a UPDATE that includes updating the disk part of the row. It
             * could be this operation or a previous operation. In both cases
             * we allocated log space for an UNDO of the UPDATE operation
             * as part of expand_tuple which happens before coming to this
             * part of the code which happens after completing the updates
             * on the row.
             *
             * The second variant is to get here starting with a DELETE
             * operation on the row followed by an INSERT of the row
             * again. This INSERT could also have been followed up with
             * an UPDATE operation. In all cases we will arrive here
             * with log space allocated in the DELETE operation that is
             * calculated based on the size of the row.
             *
             * Thus in both cases we have allocated log space for the UNDO
             * of the free of this disk row. The size of this UNDO will be
             * based on the size of the row at start of the transaction.
             * This is the size set at in initial UPDATE operation in
             * expand_tuple which is thus large enough. The size of the
             * allocated log space after an initial DELETE operation is
             * larger than required. We could free this space up here, but
             * it will only be of any help in extreme overload situations.
             *
             * The extra log space required for adding a new disk row is
             * always the same size and the state DISK_REORG will always
             * have an additional log space allocated for this purpose.
             * If we follow up this operation with a subsequent DELETE
             * operation we will free this extra log space and free the
             * new disk row and remove the DISK_REORG flag.
             */
            ndbrequire(regOperPtr->m_undo_buffer_space > 0);
            jamDataDebug(regOperPtr->m_undo_buffer_space);
            if (regOperPtr->m_uncommitted_used_space > 0)
            {
              /**
               * We have allocated space on the original row page.
               * We need to release this extra memory already now.
               * The memory used by the original row will be released
               * at commit time if the operation is committed.
               *
               * Normally we would keep the memory allocated extra on
               * the disk page until either a full abort or a commit
               * of the transaction. This is to avoid complex abort
               * handling. However changing to DISK_REORG is a
               * persistent operation lasting the rest of the transaction
               * and it is thus safe to deallocate the extra space used
               * by the old row since we have committed to using a new
               * row if we commit whatever type of operations happens
               * after this.
               */
              jam();
              Int32 overflow_space = -regOperPtr->m_uncommitted_used_space;
              ndbrequire(diskPagePtr.p->m_restart_seq == globalData.m_restart_seq);
              disk_page_dirty_header(signal,
                                     regFragPtr,
                                     key,
                                     diskPagePtr,
                                     overflow_space);
              regOperPtr->m_uncommitted_used_space = 0;
            }
            Local_key new_key;
            /* Add extra word to handle possible directory size increase.*/
            new_size++;
            int ret = disk_page_prealloc(signal,
                                         prepare_fragptr,
                                         regTabPtr,
                                         &new_key,
                                         new_size);
            if (unlikely(ret < 0))
            {
              jam();
              terrorCode = -ret;
              /**
               * Need to release log space since only recalled through
               * setting DISK_REORG bit. This bit is only set in a
               * successful prepare operation. Thus we need only abort
               * when the entire transaction is aborted.
               */
              Uint32 undo_insert_len = sizeof(Dbtup::Disk_undo::Alloc) >> 2;
              Logfile_client lgman(this,
                                   c_lgman,
                                   regFragPtr->m_logfile_group_id);
              lgman.free_log_space(undo_insert_len, jamBuffer());
              return ret;
            }
            jamDebug();
            jamDataDebug(new_key.m_file_no);
            jamDataDebug(new_key.m_page_no);
            bits |= Tuple_header::DISK_REORG;
            regOperPtr->op_struct.bit_field.m_load_extra_diskpage_on_commit= 1;
            m_base_header_bits = bits;
            new_key.m_page_idx = new_size;
	    void *ptr = (void*)req_struct->m_tuple_ptr->get_disk_ref_ptr(regTabPtr);
            memcpy(ptr, &new_key, sizeof(new_key));
            DEB_REORG(("(%u) REORG set: tab(%u,%u), row(%u,%u), disk_key:"
                       " (%u,%u), new_size: %u, undo_buffer_space: %u",
                       instance(),
                       req_struct->fragPtrP->fragTableId,
                       req_struct->fragPtrP->fragmentId,
                       regOperPtr->m_tuple_location.m_page_no,
                       regOperPtr->m_tuple_location.m_page_idx,
                       new_key.m_file_no,
                       new_key.m_page_no,
                       new_size,
                       regOperPtr->m_undo_buffer_space));
          }
        }
      }
      return 0;
    }
    Ptr<Page> pagePtr = req_struct->m_varpart_page_ptr[MM];
    Var_page* pageP= (Var_page*)pagePtr.p;
    Var_part_ref *refptr= org->get_var_part_ref_ptr(regTabPtr);
    ndbassert(! (bits & Tuple_header::COPY_TUPLE));

    Local_key ref;
    refptr->copyout(&ref);
    Uint32 alloc;
    Uint32 idx= ref.m_page_idx;
    if (bits & Tuple_header::VAR_PART)
    {
      jamDebug();
      if (copy_bits & Tuple_header::COPY_TUPLE)
      {
        jamDebug();
        ndbrequire(c_page_pool.getPtr(pagePtr, ref.m_page_no));
        pageP = (Var_page*)pagePtr.p;
      }
      alloc = pageP->get_entry_len(idx);
    }
    else
    {
      jamDebug();
      alloc = 0;
    }
    Uint32 orig_size= alloc;
    if(bits & Tuple_header::MM_GROWN)
    {
      jamDebug();
      /* Was grown before, so must fetch real original size from last word. */
      Uint32 *old_var_part= pageP->get_ptr(idx);
      ndbassert(alloc>0);
      orig_size= old_var_part[alloc-1];
    }

    if (alloc)
    {
      jamDebug();
#ifdef VM_TRACE
      if(!pageP->get_entry_chain(idx))
        ndbout << *pageP << endl;
#endif
      ndbassert(pageP->get_entry_chain(idx));
    }

    Uint32 needed= sizes[2+MM];

    if(needed <= alloc)
    {
      jam();
      continue;
    }
    /**
     * Reallocation of the variable sized part of the row is intruding
     * on all readers from the query thread. It reorganises the rows
     * visible to the readers and it can even reorganise an entire
     * page.
     *
     * This can be solved in a number of ways. One could use some kind of
     * read-write mutex on the TUP fragment in the same fashion as the
     * protection of the table fragment.
     *
     * This kind of rearrangement happens when one grows the total size
     * of the variable sized part of the row. In addition there should
     * not be any space at the end of the page. This should be rare
     * enough such that we can simply upgrade ourselves to use an
     * exclusive fragment access during the time we perform this
     * reallocation of the variable sized part.
     *
     * An alternative approach is to never touch the varsized pages until
     * we commit the operation. This would have the advantage that we need
     * no exclusive access, it would be sufficient to lock the fragment with
     * mutexes to increase the uncommitted used space or the allocation of a
     * new variable sized part.
     *
     * There is a slight advantage in such an approach in somewhat less
     * impact on concurrency. Thus if concurrency is deemed to be something
     * required to increase one could change the approach taken here.
     *
     * However the current approach has a more efficient use of memory.
     * The other approach using uncommitted used space requires holding on
     * to the old row plus allocating space for the new row in a new page.
     * Thus we could end up holding much more memory allocated during the
     * transaction compared to the approach of instantly moving the row
     * data to a new row. The current approach has more likelihood of
     * succeeding transactions in a memory constrained environment.
     *
     * The current approach will never have more memory space allocated
     * than will be required when the transaction commits. The other
     * approach if postponing the reorganisation until Commit time could
     * lead to extra memory being held during the transaction for all
     * rows that need to move to another page.
     *
     * A potential improvement here is to avoid reorganising the page
     * here and postpone this to the Commit phase. It should be sufficient
     * to remove space from the free space in the page and this should be
     * safe to do here without extra mutexes since only reads can execute
     * in parallel with this operation.
     *
     * The extra complexity required here is to track free space allocation
     * per operation. This is required to ensure that we can return the
     * free space if the operation aborts. As usual multi-operation
     * complicates matters a bit in this case since it is only the last
     * operation that worked on the row that should track the free space
     * usage.
     */
    Uint32 add = needed - alloc;
    Local_key oldref;
    refptr->copyout(&oldref);
    /**
     * Important to check alloc == 0 first since if this is true then
     * pageP is not initialised and points to garbage.
     */
    bool require_exclusive_access =
      alloc == 0 ||
      pageP->free_space < add ||
      !pageP->is_space_behind_entry(oldref.m_page_idx, add);
    if (require_exclusive_access)
    {
      jam();
      DEB_ELEM_COUNT(("(%u) realloc_var_part tab(%u,%u), var_lkey: (%u,%u),"
                      " alloc: %u, needed: %u",
                      instance(),
                      regFragPtr->fragTableId,
                      regFragPtr->fragmentId,
                      oldref.m_page_no,
                      oldref.m_page_idx,
                      alloc,
                      needed));
      c_lqh->upgrade_to_exclusive_frag_access();
      Uint32 *new_var_part=realloc_var_part(&terrorCode,
                                            regFragPtr, regTabPtr, pagePtr,
                                            refptr, alloc, needed);
      if (unlikely(new_var_part==NULL))
      {
        jam();
        c_lqh->reset_old_fragment_lock_status();
        return -1;
      }
      /* Mark the tuple grown, store the original length at the end. */
      DEB_LCP(("tab(%u,%u), row_id(%u,%u), set MM_GROWN",
              req_struct->fragPtrP->fragTableId,
              req_struct->fragPtrP->fragmentId,
              regOperPtr->m_tuple_location.m_page_no,
              regOperPtr->m_tuple_location.m_page_idx));
      org->m_header_bits= bits |
                          Tuple_header::MM_GROWN |
                          Tuple_header::VAR_PART;
      m_base_header_bits = org->m_header_bits;
      new_var_part[needed-1]= orig_size;

      /**
       * Here we can change both header bits and the reference to the varpart,
       * this means that we need to completely recalculate the checksum here.
       *
       * The source row is changed, this requires protection against readers.
       *
       * When reading we acquire the pointers to the variable parts when we
       * call prepare_read, thus it is sufficient to protect this part with
       * a mutex, we need not hold the mutex during the entire read operation.
       * It is vital to not use the row reference to the variable part after
       * releasing the mutex in query threads.
       */
      setChecksum(org, regTabPtr);
      c_lqh->downgrade_from_exclusive_frag_access();
    }
    else
    {
      jamDebug();
      /**
       * Growing the entry in the page requires not exclusive access.
       * Only LDM threads are allowed to perform Updates that will
       * change the size of pages and the free area. Thus only one
       * thread can be active on the fragment for updates. Readers can
       * can execute in parallel with this on the fragment since
       * they will only read the pointer to the varsized row part and
       * it will be ok to see both the before value and the after
       * value of the write of the index value. The index value is
       * a 32-bit value which is atomic in that the CPU will see
       * either the before value or the after value.
       */
      Uint32 *new_var_part = pageP->get_ptr(oldref.m_page_idx);
      regFragPtr->m_varWordsFree -= pageP->free_space;
      pageP->grow_entry(oldref.m_page_idx, add);
      update_free_page_list(regFragPtr, pagePtr);
      m_base_header_bits= bits |
                          Tuple_header::MM_GROWN |
                          Tuple_header::VAR_PART;
      new_var_part[needed-1]= orig_size;
    }
  }
  return 0;
}

int
Dbtup::optimize_var_part(KeyReqStruct* req_struct,
                         Tuple_header* org,
                         Operationrec* regOperPtr,
                         Fragrecord* regFragPtr,
                         Tablerec* regTabPtr)
{
  jam();
  Var_part_ref* refptr = org->get_var_part_ref_ptr(regTabPtr);

  Local_key ref;
  refptr->copyout(&ref);
  Uint32 idx = ref.m_page_idx;

  Ptr<Page> pagePtr;
  ndbrequire(c_page_pool.getPtr(pagePtr, ref.m_page_no));

  Var_page* pageP = (Var_page*)pagePtr.p;
  Uint32 var_part_size = pageP->get_entry_len(idx);

  /**
   * if the size of page list_index is MAX_FREE_LIST,
   * we think it as full page, then need not optimize
   */
  if (pageP->list_index != MAX_FREE_LIST)
  {
    jam();
    /*
     * optimize var part of tuple by moving varpart, 
     * then we possibly reclaim free pages
     */
    move_var_part(regFragPtr,
                  regTabPtr,
                  pagePtr,
                  refptr,
                  var_part_size,
                  org);
  }

  return 0;
}

int
Dbtup::nr_update_gci(Uint64 fragPtrI,
                     const Local_key* key,
                     Uint32 gci,
                     bool tuple_exists)
{
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i= fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  /**
   * GCI on the row is mandatory since many versions back.
   * During restore we have temporarily disabled this
   * flag to avoid it being set other than when done
   * with a purpose to actually set it (happens in
   * DELETE BY PAGEID and DELETE BY ROWID).
   *
   * This code is called in restore for DELETE BY
   * ROWID and PAGEID. We want to set the GCI in
   * this specific case, but not for WRITEs and
   * INSERTs, so we make this condition always
   * true.
   *
   * We don't use query threads during Copy fragment phase, thus we
   * can skip mutex protection here.
   */
  jamDebug();
  ndbrequire(!m_is_in_query_thread);
  if (tablePtr.p->m_bits & Tablerec::TR_RowGCI || true)
  {
    Local_key tmp = *key;
    PagePtr pagePtr;

    pagePtr.i = getRealpidCheck(fragPtr.p, tmp.m_page_no);
    if (unlikely(pagePtr.i == RNIL))
    {
      jam();
      ndbassert(!tuple_exists);
      return 0;
    }

    c_page_pool.getPtr(pagePtr);
    
    Tuple_header* ptr = (Tuple_header*)
      ((Fix_page*)pagePtr.p)->get_ptr(tmp.m_page_idx, 0);

    if (tuple_exists)
    {
      ndbrequire(!(ptr->m_header_bits & Tuple_header::FREE));
    }
    else
    {
      ndbrequire(ptr->m_header_bits & Tuple_header::FREE);
    }
    update_gci(fragPtr.p, tablePtr.p, ptr, gci);
  }
  return 0;
}

int
Dbtup::nr_read_pk(Uint64 fragPtrI, 
		  const Local_key* key, Uint32* dst, bool& copy)
{
  
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i= fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  Local_key tmp = *key;
  
  ndbrequire(!m_is_in_query_thread);
  PagePtr pagePtr;
  /* Mutex protection only required here for query threads. */
  pagePtr.i = getRealpidCheck(fragPtr.p, tmp.m_page_no);
  if (unlikely(pagePtr.i == RNIL))
  {
    jam();
    dst[0] = 0;
    return 0;
  }

  c_page_pool.getPtr(pagePtr);
  KeyReqStruct req_struct(this);
  Uint32* ptr= ((Fix_page*)pagePtr.p)->get_ptr(key->m_page_idx, 0);
  
  req_struct.m_lqh = c_lqh;
  req_struct.m_page_ptr = pagePtr;
  req_struct.m_tuple_ptr = (Tuple_header*)ptr;
  Uint32 bits = req_struct.m_tuple_ptr->m_header_bits;

  int ret = 0;
  copy = false;
  if (! (bits & Tuple_header::FREE))
  {
    if (bits & Tuple_header::ALLOC)
    {
      OperationrecPtr opPtr;
      opPtr.i = req_struct.m_tuple_ptr->m_operation_ptr_i;
      ndbrequire(m_curr_tup->c_operation_pool.getValidPtr(opPtr));
      ndbassert(!opPtr.p->m_copy_tuple_location.isNull());
      req_struct.m_tuple_ptr=
        get_copy_tuple(&opPtr.p->m_copy_tuple_location);
      copy = true;
    }
    Uint32 *tab_descr = tablePtr.p->tabDescriptor;
    req_struct.check_offset[MM]= tablePtr.p->get_check_offset(MM);
    req_struct.check_offset[DD]= tablePtr.p->get_check_offset(DD);
    req_struct.attr_descr= tab_descr;

    if (tablePtr.p->need_expand())
      prepare_read(&req_struct, tablePtr.p, false);
    
    const Uint32* attrIds = tablePtr.p->readKeyArray;
    const Uint32 numAttrs= tablePtr.p->noOfKeyAttr;
    // read pk attributes from original tuple
    
    req_struct.tablePtrP = tablePtr.p;
    req_struct.fragPtrP = fragPtr.p;
    
    // do it
    ret = readAttributes(&req_struct,
			 attrIds,
			 numAttrs,
			 dst,
			 ZNIL);
    
    // done
    if (likely(ret >= 0)) {
      // remove headers
      Uint32 n= 0;
      Uint32 i= 0;
      while (n < numAttrs) {
	const AttributeHeader ah(dst[i]);
	Uint32 size= ah.getDataSize();
	ndbrequire(size != 0);
	for (Uint32 j= 0; j < size; j++) {
	  dst[i + j - n]= dst[i + j + 1];
	}
	n+= 1;
	i+= 1 + size;
      }
      ndbrequire((int)i == ret);
      ret -= numAttrs;
    } else {
      return ret;
    }
  }
    
  if (tablePtr.p->m_bits & Tablerec::TR_RowGCI)
  {
    dst[ret] = *req_struct.m_tuple_ptr->get_mm_gci(tablePtr.p);
  }
  else
  {
    dst[ret] = 0;
  }
  return ret;
}

int
Dbtup::nr_delete(Signal* signal, Uint32 senderData,
		 Uint64 fragPtrI, const Local_key* key, Uint32 gci)
{
  FragrecordPtr fragPtr;
  fragPtr.i= fragPtrI;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  TablerecPtr tablePtr;
  tablePtr.i= fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  ndbrequire(!m_is_in_query_thread);
  /**
   * We execute this function as part of RESTORE operations and as part
   * of COPY fragment handling in the starting node. Thus there is no
   * concurrency from query threads that will bother us at this point in
   * time.
   */
  Local_key tmp = * key;
  tmp.m_page_no= getRealpid(fragPtr.p, tmp.m_page_no); 
  
  PagePtr pagePtr;
  Tuple_header* ptr= (Tuple_header*)get_ptr(&pagePtr, &tmp, tablePtr.p);

  if (!tablePtr.p->tuxCustomTriggers.isEmpty()) 
  {
    jam();
    TuxMaintReq* req = (TuxMaintReq*)signal->getDataPtrSend();
    req->tableId = fragPtr.p->fragTableId;
    req->fragId = fragPtr.p->fragmentId;
    req->pageId = tmp.m_page_no;
    req->pageIndex = tmp.m_page_idx;
    req->tupVersion = ptr->get_tuple_version();
    req->opInfo = TuxMaintReq::OpRemove;
    removeTuxEntries(signal, tablePtr.p);
  }
  
  Local_key disk;
  memcpy(&disk, ptr->get_disk_ref_ptr(tablePtr.p), sizeof(disk));

  Uint32 lcpScan_ptr_i= fragPtr.p->m_lcp_scan_op;
  Uint32 bits = ptr->m_header_bits;
  if (lcpScan_ptr_i != RNIL &&
      ! (bits & (Tuple_header::LCP_SKIP |
                 Tuple_header::LCP_DELETE |
                 Tuple_header::ALLOC)))
  {
    /**
     * We are performing a node restart currently, at the same time we
     * are also running a LCP on the fragment. This can happen when the
     * UNDO log level becomes too high. In this case we can start a full
     * local LCP during the copy fragment process.
     *
     * Since we are about to delete a row now, we have to ensure that the
     * lcp keep list gets this row before we delete it. This will ensure
     * that the LCP becomes a consistent LCP based on what was there at the
     * start of the LCP.
     */
    jam();
    ScanOpPtr scanOp;
    scanOp.i = lcpScan_ptr_i;
    ndbrequire(c_scanOpPool.getValidPtr(scanOp));
    if (is_rowid_in_remaining_lcp_set(pagePtr.p,
                                      fragPtr.p,
                                      *key,
                                      *scanOp.p,
                                      0))
    {
      KeyReqStruct req_struct(jamBuffer(), KRS_PREPARE);
      req_struct.m_lqh = c_lqh;
      req_struct.fragPtrP = fragPtr.p;
      Operationrec oprec;
      Tuple_header *copy;
      if ((copy = alloc_copy_tuple(tablePtr.p,
                                   &oprec.m_copy_tuple_location)) == 0)
      {
        /**
         * We failed to allocate the copy record, this is a critical error,
         * we will fail with an error message instruction to increase
         * SharedGlobalMemory.
         */
        char buf[256];
        BaseString::snprintf(buf, sizeof(buf),
                             "Out of memory when allocating copy tuple for"
                             " LCP keep list, increase SharedGlobalMemory");
        progError(__LINE__,
                  NDBD_EXIT_RESOURCE_ALLOC_ERROR,
                  buf);
      }
      req_struct.m_tuple_ptr = ptr;
      oprec.m_tuple_location = tmp;
      oprec.op_type = ZDELETE;
      DEB_LCP_SKIP_DELETE(("(%u)nr_delete: tab(%u,%u), row(%u,%u),"
                           " handle_lcp_keep_commit"
                           ", set LCP_SKIP, bits: %x",
                           instance(),
                           fragPtr.p->fragTableId,
                           fragPtr.p->fragmentId,
                           key->m_page_no,
                           key->m_page_idx,
                           bits));
      handle_lcp_keep_commit(key,
                             &req_struct,
                             &oprec,
                             fragPtr.p,
                             tablePtr.p);
      jamDebug();
      acquire_frag_mutex(fragPtr.p, key->m_page_no, jamBuffer());
      ptr->m_header_bits |= Tuple_header::LCP_SKIP;
      /**
       * Updating checksum of stored row requires protection against
       * readers in other threads.
       */
      updateChecksum(ptr, tablePtr.p, bits, ptr->m_header_bits);
      release_frag_mutex(fragPtr.p, key->m_page_no, jamBuffer());
    }
  }

  /**
   * A row is deleted as part of Copy fragment or Restore
   * We need to keep track of the row count also during restore.
   * We increment number of changed rows, for restore this variable
   * will be cleared after completing the restore, but it is
   * important to count it while performing a COPY fragment
   * operation.
   */
  fragPtr.p->m_row_count--;
  fragPtr.p->m_lcp_changed_rows++;

  DEB_DELETE_NR(("(%u)nr_delete, tab(%u,%u) row(%u,%u), gci: %u"
                 ", row_count: %llu",
                 instance(),
                 fragPtr.p->fragTableId,
                 fragPtr.p->fragmentId,
                 key->m_page_no,
                 key->m_page_idx,
                 *ptr->get_mm_gci(tablePtr.p),
                 fragPtr.p->m_row_count));

  /**
   * No query threads active when restore and copy fragment process
   * is active. Thus no need to lock mutex here.
   */
  if (tablePtr.p->m_attributes[MM].m_no_of_varsize +
      tablePtr.p->m_attributes[MM].m_no_of_dynamic)
  {
    jam();
    free_var_rec(fragPtr.p, tablePtr.p, &tmp, pagePtr);
  } else {
    jam();
    free_fix_rec(fragPtr.p, tablePtr.p, &tmp, (Fix_page*)pagePtr.p);
  }

  if (tablePtr.p->m_no_of_disk_attributes)
  {
    jam();
    Ptr<GlobalPage> diskPagePtr;
    int res;
    Uint32 sz;
    Uint32 page_idx;
    Uint32 entry_len;
    Uint32 size_len;

    /**
     * 1) get page
     * 2) alloc log buffer
     * 3) get log buffer
     * 4) delete tuple
     */
    Page_cache_client::Request preq;
    preq.m_page = disk;
    preq.m_table_id = fragPtr.p->fragTableId;
    preq.m_fragment_id = fragPtr.p->fragmentId;
    preq.m_callback.m_callbackData = senderData;
    preq.m_callback.m_callbackFunction =
      safe_cast(&Dbtup::nr_delete_page_callback);
    int flags = Page_cache_client::COMMIT_REQ;
    
#ifdef ERROR_INSERT
    if (ERROR_INSERTED(4023) || ERROR_INSERTED(4024))
    {
      int rnd = rand() % 100;
      int slp = 0;
      if (ERROR_INSERTED(4024))
      {
	slp = 3000;
      }
      else if (rnd > 90)
      {
	slp = 3000;
      }
      else if (rnd > 70)
      {
	slp = 100;
      }

      g_eventLogger->info("rnd: %d slp: %d", rnd, slp);

      if (slp)
      {
	flags |= Page_cache_client::DELAY_REQ;
        const NDB_TICKS now = NdbTick_getCurrentTicks();
        preq.m_delay_until_time = NdbTick_AddMilliseconds(now,(Uint64)slp);
      }
    }
#endif
    {
      Page_cache_client pgman(this, c_pgman);
      res = pgman.get_page(signal, preq, flags);
      diskPagePtr = pgman.m_ptr;
      if (res == 0)
      {
        goto timeslice;
      }
      /**
       * We are processing node recovery and need to process a disk
       * data page, if this fails we cannot proceed with node recovery.
       */
      ndbrequire(res > 0);

      if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
      {
        jam();
        sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
          tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
      }
      else
      {
        jam();
        page_idx = disk.m_page_idx;
        entry_len = ((Var_page*)diskPagePtr.p)->get_entry_len(page_idx);
        size_len = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2);
        sz = size_len + (entry_len - 1);

      }
      D("Logfile_client - nr_delete");
      {
        Logfile_client lgman(this, c_lgman, fragPtr.p->m_logfile_group_id);
        res = lgman.alloc_log_space(sz, false, false, jamBuffer());
        ndbrequire(res == 0);
        /* Complete work on LGMAN before setting page to dirty */
        CallbackPtr cptr;
        cptr.m_callbackIndex = NR_DELETE_LOG_BUFFER_CALLBACK;
        cptr.m_callbackData = senderData;
        res= lgman.get_log_buffer(signal, sz, &cptr);
      }
    } // Unlock the LGMAN lock

    PagePtr disk_page((Tup_page*)diskPagePtr.p, diskPagePtr.i);
    disk_page_set_dirty(disk_page, fragPtr.p);

    switch(res){
    case 0:
      signal->theData[2] = disk_page.i;
      goto timeslice;
    case -1:
      ndbrequire("NOT YET IMPLEMENTED" == 0);
      break;
    }
    disk_page_free(signal,
                   tablePtr.p,
                   fragPtr.p,
		   &disk,
                   *(PagePtr*)&disk_page,
                   gci,
                   key,
                   sz);
    return 0;
  }
  
  return 0;

timeslice:
  memcpy(signal->theData, &disk, sizeof(disk));
  return 1;
}

void
Dbtup::nr_delete_page_callback(Signal* signal, 
			       Uint32 userpointer, Uint32 page_id)//unused
{
  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, page_id));
  PagePtr pagePtr((Tup_page*)gpage.p, gpage.i);
  Dblqh::Nr_op_info op;
  op.m_ptr_i = userpointer;
  jam();
  jamData(userpointer);
  op.m_disk_ref.m_page_no = pagePtr.p->m_page_no;
  op.m_disk_ref.m_file_no = pagePtr.p->m_file_no;
  c_lqh->get_nr_op_info(&op, page_id);

  FragrecordPtr fragPtr;
  fragPtr.i= op.m_tup_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));
  disk_page_set_dirty(pagePtr, fragPtr.p);

  Ptr<Tablerec> tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);
  
  Uint32 sz;
  if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
      tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
  }
  else
  {
    Uint32 page_idx = op.m_disk_ref.m_page_idx;
    Uint32 entry_len = ((Var_page*)pagePtr.p)->get_entry_len(page_idx);
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
           (entry_len - 1);
  }
  int res;
  {
    Logfile_client lgman(this, c_lgman, fragPtr.p->m_logfile_group_id);
    res = lgman.alloc_log_space(sz, false, false, jamBuffer());
    ndbrequire(res == 0);

    CallbackPtr cb;
    cb.m_callbackData = userpointer;
    cb.m_callbackIndex = NR_DELETE_LOG_BUFFER_CALLBACK;
    D("Logfile_client - nr_delete_page_callback");
    res= lgman.get_log_buffer(signal, sz, &cb);
  }
  switch(res){
  case 0:
    jam();
    return;
  case -1:
    ndbrequire("NOT YET IMPLEMENTED" == 0);
    break;
  }
  jam();
  disk_page_free(signal,
                 tablePtr.p, fragPtr.p,
		 &op.m_disk_ref,
                 pagePtr,
                 op.m_gci_hi,
                 &op.m_row_id,
                 sz);
  
  c_lqh->nr_delete_complete(signal, &op);
  return;
}

void
Dbtup::nr_delete_log_buffer_callback(Signal* signal, 
				    Uint32 userpointer, 
				    Uint32 unused)
{
  Dblqh::Nr_op_info op;
  op.m_ptr_i = userpointer;
  jam();
  jamData(userpointer);
  c_lqh->get_nr_op_info(&op, RNIL);
  
  FragrecordPtr fragPtr;
  fragPtr.i= op.m_tup_frag_ptr_i;
  ndbrequire(c_fragment_pool.getPtr(fragPtr));

  Ptr<Tablerec> tablePtr;
  tablePtr.i = fragPtr.p->fragTableId;
  ptrCheckGuard(tablePtr, cnoOfTablerec, tablerec);

  Ptr<GlobalPage> gpage;
  ndbrequire(m_global_page_pool.getPtr(gpage, op.m_page_id));
  PagePtr pagePtr((Tup_page*)gpage.p, gpage.i);

  Uint32 sz;
  if ((tablePtr.p->m_bits & Tablerec::TR_UseVarSizedDiskData) == 0)
  {
    jam();
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) + 
      tablePtr.p->m_offsets[DD].m_fix_header_size - 1;
  }
  else
  {
    jam();
    Uint32 page_idx = op.m_disk_ref.m_page_idx;
    Uint32 entry_len = ((Var_page*)pagePtr.p)->get_entry_len(page_idx);
    sz = (sizeof(Dbtup::Disk_undo::Update_Free) >> 2) +
           (entry_len - 1);
  }

  /**
   * reset page no
   */
  disk_page_free(signal,
                 tablePtr.p,
                 fragPtr.p,
		 &op.m_disk_ref,
                 pagePtr,
                 op.m_gci_hi,
                 &op.m_row_id,
                 sz);
  
  c_lqh->nr_delete_complete(signal, &op);
}
