/*
   Copyright (c) 2003, 2025, Oracle and/or its affiliates.
   Copyright (c) 2023, 2025, Hopsworks and/or its affiliates.

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
#define DBTUP_BUFFER_CPP
#include <ndb_limits.h>
#include <RefConvert.hpp>
#include <pc.hpp>
#include <signaldata/TransIdAI.hpp>
#include "Dbtup.hpp"
#include "../dblqh/Dblqh.hpp"
#include "AggInterpreter.hpp"

#define JAM_FILE_ID 410

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_CONT_SCAN 1
//#define DEBUG_TRANSID_AI 1
#endif

#ifdef DEBUG_TRANSID_AI
#define DEB_TRANSID_AI(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_TRANSID_AI(arglist) do { } while (0)
#endif

#ifdef DEBUG_CONT_SCAN
#define DEB_CONT_SCAN(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_CONT_SCAN(arglist) do { } while (0)
#endif

void Dbtup::execSEND_PACKED(Signal *signal) {
  Uint16 hostId;
  Uint32 i;
  Uint32 TpackedListIndex = cpackedListIndex;
  bool present = false;
  for (i = 0; i < TpackedListIndex; i++) {
    jam();
    hostId = cpackedList[i];
    ndbrequire(Uint32((hostId - 1)) < Uint32((MAX_NODES - 1)));  // Also check not zero
    HostBuffer *const buffer = &hostBuffer[hostId];
    Uint32 TpacketTA = buffer->noOfPacketsTA;
    if (TpacketTA != 0) {
      jamDebug();

      if (ERROR_INSERTED(4037)) {
        /* Delay a SEND_PACKED signal for 10 calls to execSEND_PACKED */
        jam();
        if (!present) {
          /* First valid packed data in this pass */
          jamDebug();
          present = true;
          cerrorPackedDelay++;

          if ((cerrorPackedDelay % 10) != 0) {
            /* Skip it */
            jamDebug();
            return;
          }
        }
      }
      const BlockReference TBref = numberToRef(API_PACKED, hostId);
      const Uint32 TpacketLen = buffer->packetLenTA;
      MEMCOPY_NO_WORDS(&signal->theData[0], &buffer->packetBufferTA[0],
                       TpacketLen);
      sendSignal(TBref, GSN_TRANSID_AI, signal, TpacketLen, JBB);
      buffer->noOfPacketsTA = 0;
      buffer->packetLenTA = 0;
    }
    buffer->inPackedList = false;
  }  // for
  cpackedListIndex = 0;
}

/**
 * Copy a TRANSID_AI signal, which already has its header constructed in
 * 'signal', into a packed buffer structure.
 *
 * Prereq:
 *  - Signal should be sufficiently small to allow it to be 'packed'
 *  - Buffer should have sufficient free space for the signal.
 */
void Dbtup::bufferTRANSID_AI(Signal *signal, BlockReference aRef,
                             const Uint32 *dataBuf, Uint32 lenOfData) {
  ndbassert(lenOfData > 0);
  ndbassert(TransIdAI::HeaderLength + lenOfData + 1 <= 25);

  const Uint32 hostId = refToNode(aRef);
  HostBuffer *const buffer = &hostBuffer[hostId];
  const Uint32 TpacketLen = buffer->packetLenTA;

  // ----------------------------------------------------------------
  // There should always be space in the buffer.
  // ----------------------------------------------------------------
  ndbassert((TpacketLen + 1 + TransIdAI::HeaderLength + lenOfData) <= 25);

  // ----------------------------------------------------------------
  // Copy the header + TRANSID_AI signal into the buffer
  // ----------------------------------------------------------------
  Uint32 *const packedBuffer = &buffer->packetBufferTA[TpacketLen];
  const Uint32 Theader = ((refToBlock(aRef) << 16) + lenOfData);
  packedBuffer[0] = Theader;

  MEMCOPY_NO_WORDS(&packedBuffer[1], signal->theData, TransIdAI::HeaderLength);
  MEMCOPY_NO_WORDS(&packedBuffer[1 + TransIdAI::HeaderLength], dataBuf,
                   lenOfData);

  buffer->packetLenTA = TpacketLen + 1 + TransIdAI::HeaderLength + lenOfData;
  buffer->noOfPacketsTA++;
  updatePackedList(hostId);
}

void Dbtup::updatePackedList(Uint16 hostId) {
  if (hostBuffer[hostId].inPackedList == false) {
    Uint32 TpackedListIndex = cpackedListIndex;
    jamDebug();
    hostBuffer[hostId].inPackedList = true;
    cpackedList[TpackedListIndex] = hostId;
    cpackedListIndex = TpackedListIndex + 1;
  }
}

void Dbtup::print_checksum(const Uint32 *data,
                           const Uint32 ref,
                           const Uint32 len,
                           const Uint32 line) {
  Uint32 checksum = 0;
  for (Uint32 i = 0; i < len; i++) {
    checksum ^= data[i];
  }
  g_eventLogger->info("(%u) SEND TRANSID_AI: checksum: 0x%x, len: %u,"
                      " ref: 0x%x, line: %u",
    instance(), checksum, len, ref, line);
}

/**
 * Send a TRANSID_AI signal to an API node. If sufficiently small, the signal is
 * buffered for later being sent as a API_PACKED-signal. When required the
 * packed buffer is flushed to the destination API-node.
 *
 * Prereq:
 *  - The destination node must be an API node.
 *  - We must be connected to the API node.
 */
void Dbtup::sendAPI_TRANSID_AI(Signal *signal, Uint32 recBlockRef,
                               const Uint32 *dataBuf, Uint32 lenOfData,
                               KeyReqStruct *req_struct) {
  const Uint32 nodeId = refToNode(recBlockRef);

  // Test prerequisites:
  ndbassert(getNodeInfo(nodeId).m_type >= NodeInfo::API &&
            getNodeInfo(nodeId).m_type <= NodeInfo::MGM);

  ndbrequire(nodeId < MAX_NODES);
  HostBuffer *const buffer = &hostBuffer[nodeId];
  const Uint32 TpacketLen = buffer->packetLenTA;

  ndbassert(dataBuf == &signal->theData[25]);

  /**
   * Check if the packed buffers has to be flushed first.
   * Note that even if we will not use them for this (too large) signal,
   * it has to be flushed now in order to maintain the order of TRANSID_AIs
   */
  if (TpacketLen > 0 &&
      TpacketLen + 1 + TransIdAI::HeaderLength + lenOfData > 25) {
    jamDebug();
    TransIdAI *transIdAI = (TransIdAI *)signal->getDataPtrSend();

    // Save prepare TRANSID_AI header
    const Uint32 sig0 = transIdAI->connectPtr;
    const Uint32 sig1 = transIdAI->transId[0];
    const Uint32 sig2 = transIdAI->transId[1];


    // Send already buffered TRANSID_AI(s) preceding this TRANSID_AI
    const BlockReference TBref = numberToRef(API_PACKED, nodeId);
    MEMCOPY_NO_WORDS(&signal->theData[0], &buffer->packetBufferTA[0],
                     TpacketLen);
    sendSignal(TBref, GSN_TRANSID_AI, signal, TpacketLen, JBB);
    buffer->noOfPacketsTA = 0;
    buffer->packetLenTA = 0;

    // Reconstruct the current TRANSID_AI header
    transIdAI->connectPtr = sig0;
    transIdAI->transId[0] = sig1;
    transIdAI->transId[1] = sig2;
  }
#ifdef DEBUG_TRANSID_AI
  print_checksum(dataBuf, recBlockRef, lenOfData, __LINE__);
#endif
  if (lenOfData <= TransIdAI::DataLength) {
    /**
     * Short signal, buffer it, or send directly
     * 1) Buffer signal if we can pack at least
     *    this signal + another 1-word signal into buffers.
     * 2) else, short-signal is sent immediately.
     *
     * Note that the check for fitting a 1-word signal in addition
     * to this signal serves dual purposes:
     * - The 1-word signal is the smallest possible signal which
     *   can either be added later, or already is buffered.
     * - So failing to also add a 1-word signal implies that any
     *   previously buffered signals were flushed above.
     *   Thus, 'packetLenTA' is also known to be '== 0' in
     *   the non-buffered sendSignal further below.
     */
#ifndef NDB_NO_DROPPED_SIGNAL
    if (1 + TransIdAI::HeaderLength + lenOfData +  // this TRANSID_AI
            1 + TransIdAI::HeaderLength + 1 <=
        25)  // 1 word TRANSID_AI
    {
      jamDebug();
      bufferTRANSID_AI(signal, recBlockRef, dataBuf, lenOfData);
    } else
#endif
    {
      jamDebug();
      ndbassert(buffer->packetLenTA == 0);
      MEMCOPY_NO_WORDS(&signal->theData[TransIdAI::HeaderLength], dataBuf,
                       lenOfData);
      DEB_TRANSID_AI(("(%u) Send API AI Short0: len: %u, ref: 0x%x, map: %u",
        instance(), lenOfData, recBlockRef, signal->theData[0]));
      sendSignal(recBlockRef, GSN_TRANSID_AI, signal,
                 TransIdAI::HeaderLength + lenOfData, JBB);
    }
    return;
  } else {
    jamDebug();
    /**
     * Send to API as a long signal.
     */
    LinearSectionPtr ptr[3];
    ptr[0].p = const_cast<Uint32 *>(dataBuf);
    ptr[0].sz = lenOfData;
    if (likely(lenOfData <= MAX_TRANSID_AI_SIZE)) {
      DEB_TRANSID_AI(("(%u) Send API AI Short1: len: %u, ref: 0x%x, map: %u",
        instance(), lenOfData, recBlockRef, signal->theData[0]));
      sendSignal(recBlockRef, GSN_TRANSID_AI, signal, TransIdAI::HeaderLength,
                 JBB, ptr, 1);
    } else {
      jam();
      DEB_TRANSID_AI(("(%u) Send API AI Long: len: %u, ref: 0x%x",
        instance(), lenOfData, recBlockRef));
      TransIdAILong *transIdAILong = (TransIdAILong *)signal->getDataPtrSend();
      Uint32 sig_len = TransIdAILong::HeaderLength;
      if (req_struct->m_use_corr_factor) {
        jam();
        sig_len = TransIdAILong::HeaderWithCorrelationLength;
        std::memcpy(&transIdAILong->correlationData[0],
                    &dataBuf[lenOfData - 3],
                    3 * 4);
      }
      transIdAILong->totalLen = lenOfData;
      sendBatchedFragmentedSignal(recBlockRef, GSN_TRANSID_AI, signal,
                 sig_len, JBB, ptr, 1);
    }
  }
}

/* ---------------------------------------------------------------- */
/* ----------------------- SEND READ ATTRINFO --------------------- */
/* ---------------------------------------------------------------- */
void Dbtup::sendReadAttrinfo(Signal *signal, KeyReqStruct *req_struct,
                             Uint32 ToutBufIndex) {
  if (ToutBufIndex == 0) return;

  const BlockReference recBlockref = req_struct->rec_blockref;
  ndbassert(refToMain(recBlockref) != 32770);
  const Uint32 nodeId = refToNode(recBlockref);

  bool connectedToNode = getNodeInfo(nodeId).m_connected;
  const Uint32 type = getNodeInfo(nodeId).m_type;
  const bool is_api = (type >= NodeInfo::API && type <= NodeInfo::MGM);

  if (ERROR_INSERTED(4006) && (nodeId != getOwnNodeId())) {
    // Use error insert to turn routing on
    jam();
    connectedToNode = false;
  }

  DEB_CONT_SCAN(("(%u) TUP Sending TRANSID_AI with api_ref: %u, len: %u",
    instance(), req_struct->tc_operation_ptr, ToutBufIndex));

  Uint32 sig0 = req_struct->tc_operation_ptr;
  Uint32 sig1 = req_struct->trans_id1;
  Uint32 sig2 = req_struct->trans_id2;

  TransIdAI *transIdAI = (TransIdAI *)signal->getDataPtrSend();
  transIdAI->connectPtr = sig0;
  transIdAI->transId[0] = sig1;
  transIdAI->transId[1] = sig2;

  const Uint32 routeBlockref = req_struct->TC_ref;
  ndbassert(refToMain(routeBlockref) == DBTC ||
            refToMain(routeBlockref) == DBSPJ ||
            (nodeId == getOwnNodeId() && connectedToNode));

#ifdef DEBUG_TRANSID_AI
  print_checksum(&signal->theData[25], recBlockref, ToutBufIndex, __LINE__);
#endif
  if (req_struct->read_length != 0) {
    ndbassert(!is_api);  // API result already FLUSH_AI'ed
  } else {
    req_struct->read_length = ToutBufIndex;
  }

  /*
   * Hot path: connected to dest node, short signal (or EXECUTE_DIRECT
   * locally, or sendAPI_TRANSID_AI for API clients). These cases never
   * need a local LinearSectionPtr array, so the compiler emits no stack
   * canary here.
   *
   * Anything that needs a LinearSectionPtr — cross-node or same-node
   * long signals, ERROR_INSERT-delayed send, and the disconnected
   * TRANSID_AI_R routing path — is handled in sendReadAttrinfoSlow.
   */
  if (unlikely(!connectedToNode)) {
    sendReadAttrinfoRouted(signal, req_struct, ToutBufIndex, recBlockref,
                           routeBlockref);
    return;
  }

  if (nodeId != getOwnNodeId()) {
    jamDebug();
    if (is_api) {
      DEB_TRANSID_AI(("(%u) sendReadAttrinfo: API AI len: %u, ref: 0x%x",
        instance(), ToutBufIndex, recBlockref));
      sendAPI_TRANSID_AI(signal, recBlockref, &signal->theData[25],
                         ToutBufIndex, req_struct);
      return;
    }
    if (unlikely(ToutBufIndex > TransIdAI::DataLength)) {
      sendReadAttrinfoLong(signal, req_struct, ToutBufIndex, recBlockref,
                           nodeId);
      return;
    }
    jam();
    /* Data is 'short', send short signal */
    MEMCOPY_NO_WORDS(&signal->theData[TransIdAI::HeaderLength],
                     &signal->theData[25], ToutBufIndex);
    sendSignal(recBlockref, GSN_TRANSID_AI, signal,
               TransIdAI::HeaderLength + ToutBufIndex, JBB);
    return;
  }

  /*
   * Same node.
   * BACKUP, LQH run in our thread, so we can EXECUTE_DIRECT().
   * The UTIL/TC blocks are in another thread (in multi-threaded ndbd),
   * so must use sendSignal().
   */
  const bool sameInstance = refToInstance(recBlockref) == instance();
  const Uint32 blockNumber = refToMain(recBlockref);
  if (sameInstance &&
      (blockNumber == getBACKUP() || blockNumber == getDBLQH())) {
    memmove(&signal->theData[TransIdAI::HeaderLength],
            &signal->theData[25],
            ToutBufIndex * sizeof(Uint32));
    static_assert(MAX_TUPLE_SIZE_IN_WORDS + MAX_ATTRIBUTES_IN_TABLE <=
                  NDB_ARRAY_SIZE(signal->theData) - TransIdAI::HeaderLength);
    ndbrequire(TransIdAI::HeaderLength + ToutBufIndex <=
               NDB_ARRAY_SIZE(signal->theData));
    EXECUTE_DIRECT(blockNumber, GSN_TRANSID_AI, signal,
                   TransIdAI::HeaderLength + ToutBufIndex);
    jamEntryDebug();
    return;
  }

  if (unlikely(ToutBufIndex > TransIdAI::DataLength)) {
    sendReadAttrinfoLong(signal, req_struct, ToutBufIndex, recBlockref,
                         nodeId);
    return;
  }

  jam();
  MEMCOPY_NO_WORDS(&signal->theData[TransIdAI::HeaderLength],
                   &signal->theData[25], ToutBufIndex);
  const JobBufferLevel prioLevel = req_struct->m_prio_a_flag ? JBA : JBB;
  sendSignal(recBlockref, GSN_TRANSID_AI, signal,
             TransIdAI::HeaderLength + ToutBufIndex, prioLevel);
}

void Dbtup::sendReadAttrinfoLong(Signal *signal, KeyReqStruct *req_struct,
                                 Uint32 ToutBufIndex,
                                 BlockReference recBlockref,
                                 Uint32 nodeId) {
  LinearSectionPtr ptr[3];
  ptr[0].p = &signal->theData[25];
  ptr[0].sz = ToutBufIndex;

  if (nodeId != getOwnNodeId()) {
    /* Cross-node long signal. Receiver block doesn't support
     * packed 'short' signals. */
    jam();
    DEB_TRANSID_AI(("(%u) sendReadAttrinfo: DN AI len: %u, ref: 0x%x",
      instance(), ToutBufIndex, recBlockref));
    if (ToutBufIndex <= MAX_TRANSID_AI_SIZE) {
      jamDebug();
      sendSignal(recBlockref, GSN_TRANSID_AI, signal,
                 TransIdAI::HeaderLength, JBB, ptr, 1);
    } else {
      jam();
      jamDataDebug(ToutBufIndex);
      sendBatchedFragmentedSignal(recBlockref, GSN_TRANSID_AI, signal,
        TransIdAI::HeaderLength, JBB, ptr, 1);
    }
    return;
  }

  /* Same-node long signal (not same-instance EXECUTE_DIRECT). */
  jam();
  if (ERROR_INSERTED(4038) &&
      refToMain(recBlockref) != BACKUP &&
      ptr[0].sz <= 7500) {
    /* Copy data to Seg-section for delayed send */
    jam();
    Uint32 sectionIVal = RNIL;
    ndbrequire(appendToSection(sectionIVal, ptr[0].p, ptr[0].sz));
    SectionHandle sh(this, sectionIVal);

    sendSignalWithDelay(recBlockref, GSN_TRANSID_AI, signal, 10,
                        TransIdAI::HeaderLength, &sh);
    return;
  }
  /*
   * Same-node: maintain signal order with SCAN_FRAGCONF by sending at
   * the same priority (A if the caller's m_prio_a_flag is set).
   * Since we are within the same data node we need not communicate
   * the totalLen in the signal — the receiver figures it out in
   * assembleFragments, and setting it here would overwrite the data
   * sent.
   */
  JobBufferLevel prioLevel;
  if (!req_struct->m_prio_a_flag) {
    jam();
    prioLevel = JBB;
  } else {
    jam();
    prioLevel = JBA;
  }
#ifdef DEBUG_TRANSID_AI
  DEB_TRANSID_AI(("(%u) sendReadAttrinfo: Same DN AI len: %u, ref: 0x%x",
    instance(), ToutBufIndex, recBlockref));
  if (req_struct->m_use_corr_factor) {
    ndbrequire(req_struct->m_use_corr_factor == 1);
    Uint32 len = ToutBufIndex;
    const Uint32 *dataPtr = ptr[0].p;
    DEB_TRANSID_AI(("(%u) CorrelationData: 0x%x,0x%x",
      instance(), dataPtr[len - 2], dataPtr[len - 1]));
  }
#endif
  if (ToutBufIndex <= MAX_TRANSID_AI_SIZE) {
    jamDebug();
    sendSignal(recBlockref, GSN_TRANSID_AI, signal,
      TransIdAI::HeaderLength, prioLevel, ptr, 1);
  } else {
    jamDebug();
    /*
     * Batched signals overwrite part of the signal object to send
     * segment information, thus we have to copy the data away from
     * this area.
     */
    sendBatchedFragmentedSignal(
      recBlockref, GSN_TRANSID_AI, signal,
      TransIdAI::HeaderLength, prioLevel, ptr, 1);
  }
}

void Dbtup::sendReadAttrinfoRouted(Signal *signal, KeyReqStruct *req_struct,
                                   Uint32 ToutBufIndex,
                                   BlockReference recBlockref,
                                   BlockReference routeBlockref) {
  /*
   * No direct connection to the receiving node. Send routed via
   * the node that controls this read (TC).
   */
  if (refToNode(recBlockref) == refToNode(routeBlockref)) {
    jam();
    /* Only alternative route is direct — cannot be delivered, drop. */
    return;
  }
  ndbrequire(refToMain(routeBlockref) == DBTC);
  TransIdAI *transIdAI = (TransIdAI *)signal->getDataPtrSend();
  LinearSectionPtr ptr[3];
  ptr[0].p = &signal->theData[25];
  ptr[0].sz = ToutBufIndex;
  DEB_TRANSID_AI(("(%u) sendReadAttrinfo: AI_R len: %u, ref: 0x%x, map: %u",
    instance(), ToutBufIndex, recBlockref, signal->theData[0]));
  if (ToutBufIndex <= MAX_TRANSID_AI_SIZE) {
    jam();
    transIdAI->attrData[0] = recBlockref;
    sendSignal(routeBlockref, GSN_TRANSID_AI_R, signal,
               TransIdAI::HeaderLength + 1, JBB, ptr, 1);
  } else {
    jam();
    Uint32 sig_len;
    TransIdAILong *const transIdAILong =
        (TransIdAILong *)signal->getDataPtr();
    if (req_struct->m_use_corr_factor) {
      const Uint32 *dataPtr = ptr[0].p;
      const Uint32 len_corr = req_struct->m_use_corr_factor + 1;
      sig_len = TransIdAILong::HeaderWithCorrelationLength + 1;
      std::memcpy(&transIdAILong->correlationData[0],
                  &dataPtr[ToutBufIndex - len_corr],
                  len_corr * 4);
      transIdAILong->attrData[0] = recBlockref;
    } else {
      sig_len = TransIdAILong::HeaderLength + 1;
      transIdAILong->correlationData[0] = recBlockref;
    }
    transIdAILong->totalLen = ToutBufIndex;
    sendBatchedFragmentedSignal(routeBlockref, GSN_TRANSID_AI_R, signal,
      sig_len, JBB, ptr, 1);
  }
}

bool Dbtup::SendAggResToAPI(Signal* signal, const void* lqhTcConnectrec,
                            void* lqhScanRecord) {
  const Dblqh::TcConnectionrec* lqhOpPtrP =
                              (Dblqh::TcConnectionrec*)lqhTcConnectrec;
  // PA related
  Dblqh::ScanRecord* lqhScanPtrP = (Dblqh::ScanRecord*)lqhScanRecord;
  // m_agg_interpreter != nullptr implies m_has_pushdown == true
  ndbrequire(lqhScanPtrP->m_agg_interpreter != nullptr);
  AggInterpreter* interp = lqhScanPtrP->m_agg_interpreter;
  Uint32 res_len = interp->PrepareAggResIfNeeded(signal, true);
  const auto* gb_map = interp->gb_map();
  bool all_sent = (gb_map == nullptr || gb_map->empty());
  if (all_sent) {
    lqhScanPtrP->m_agg_n_res_recs = interp->NumOfResRecords(true);
  }
  if (res_len != 0) {
    if (all_sent) {
      ndbrequire(lqhScanPtrP->m_agg_n_res_recs == 0);
    }
    TransIdAI * transIdAI=  (TransIdAI *)signal->getDataPtrSend();
    transIdAI->connectPtr = lqhScanPtrP->scanApiOpPtr[lqhScanPtrP->scanApiOpPtr_index];
    transIdAI->transId[0] = lqhOpPtrP->transid[0];
    transIdAI->transId[1] = lqhOpPtrP->transid[1];
    ndbrequire(lqhScanPtrP->m_agg_curr_batch_size_bytes == 0);
    ndbrequire(lqhScanPtrP->m_agg_curr_batch_size_rows == 0);
    lqhScanPtrP->m_agg_curr_batch_size_bytes = res_len * sizeof(Uint32);
    lqhScanPtrP->m_agg_curr_batch_size_rows = 1;
    SendAggregationResult(signal, res_len, lqhScanPtrP->scanApiBlockref);
  }
  PA_RONDB_TRACE(lqhScanPtrP->m_has_pushdown,
      lqhOpPtrP->tableref, interp->frag_id(),
      "Dbtup::SendAggResToAPI(), "
      "End-scan, send at last, res_len: %u, all_sent: %u,"
      " trans[0]: %u, trans[2]: %u, connectPtr: %u, blockref: %u"
      ", size_rows[%u, %u], size_bytes: [%u, %u], n_res_recs: %u\n",
      res_len,
      (Uint32)all_sent,
      lqhOpPtrP->transid[0],
      lqhOpPtrP->transid[1],
      lqhScanPtrP->scanApiOpPtr[lqhScanPtrP->scanApiOpPtr_index],
      lqhScanPtrP->scanApiBlockref,
      lqhScanPtrP->m_agg_curr_batch_size_rows,
      lqhScanPtrP->m_curr_batch_size_rows,
      lqhScanPtrP->m_agg_curr_batch_size_bytes,
      lqhScanPtrP->m_curr_batch_size_bytes,
      lqhScanPtrP->m_agg_n_res_recs);
  return all_sent;
}
