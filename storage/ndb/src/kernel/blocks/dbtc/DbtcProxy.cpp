/*
   Copyright (c) 2011, 2024, Oracle and/or its affiliates.
   Copyright (c) 2021, 2024, Hopsworks and/or its affiliates.

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

#include "DbtcProxy.hpp"
#include "Dbtc.hpp"
#include "portlib/NdbMem.h"

#define JAM_FILE_ID 352

DbtcProxy::DbtcProxy(Block_context &ctx) : DbgdmProxy(DBTC, ctx) {
  // GSN_TCSEIZEREQ
  addRecSignal(GSN_TCSEIZEREQ, &DbtcProxy::execTCSEIZEREQ);

  // GSN_TCGETOPSIZEREQ
  addRecSignal(GSN_TCGETOPSIZEREQ, &DbtcProxy::execTCGETOPSIZEREQ);
  addRecSignal(GSN_TCGETOPSIZECONF, &DbtcProxy::execTCGETOPSIZECONF);

  // GSN_TCGETOPSIZEREQ
  addRecSignal(GSN_TC_CLOPSIZEREQ, &DbtcProxy::execTC_CLOPSIZEREQ);
  addRecSignal(GSN_TC_CLOPSIZECONF, &DbtcProxy::execTC_CLOPSIZECONF);

  // GSN_GCP_NOMORETRANS
  addRecSignal(GSN_GCP_NOMORETRANS, &DbtcProxy::execGCP_NOMORETRANS);
  addRecSignal(GSN_GCP_TCFINISHED, &DbtcProxy::execGCP_TCFINISHED);

  // GSN_CREATE_INDX_IMPL_REQ
  addRecSignal(GSN_CREATE_INDX_IMPL_REQ, &DbtcProxy::execCREATE_INDX_IMPL_REQ);
  addRecSignal(GSN_CREATE_INDX_IMPL_CONF,
               &DbtcProxy::execCREATE_INDX_IMPL_CONF);
  addRecSignal(GSN_CREATE_INDX_IMPL_REF, &DbtcProxy::execCREATE_INDX_IMPL_REF);

  // GSN_ALTER_INDX_IMPL_REQ
  addRecSignal(GSN_ALTER_INDX_IMPL_REQ, &DbtcProxy::execALTER_INDX_IMPL_REQ);
  addRecSignal(GSN_ALTER_INDX_IMPL_CONF, &DbtcProxy::execALTER_INDX_IMPL_CONF);
  addRecSignal(GSN_ALTER_INDX_IMPL_REF, &DbtcProxy::execALTER_INDX_IMPL_REF);

  // GSN_DROP_INDX_IMPL_REQ
  addRecSignal(GSN_DROP_INDX_IMPL_REQ, &DbtcProxy::execDROP_INDX_IMPL_REQ);
  addRecSignal(GSN_DROP_INDX_IMPL_CONF, &DbtcProxy::execDROP_INDX_IMPL_CONF);
  addRecSignal(GSN_DROP_INDX_IMPL_REF, &DbtcProxy::execDROP_INDX_IMPL_REF);

  // GSN_TAKE_OVERTCCONF
  addRecSignal(GSN_TAKE_OVERTCCONF, &DbtcProxy::execTAKE_OVERTCCONF);

  // GSN_ABORT_ALL_REQ
  addRecSignal(GSN_ABORT_ALL_REQ, &DbtcProxy::execABORT_ALL_REQ);
  addRecSignal(GSN_ABORT_ALL_REF, &DbtcProxy::execABORT_ALL_REF);
  addRecSignal(GSN_ABORT_ALL_CONF, &DbtcProxy::execABORT_ALL_CONF);

  // Routed signals are distributed across the workers
  // This requires that there's no ordering constraints between them
  // GSN_TCKEY_FAILREFCONF_R
  addRecSignal(GSN_TCKEY_FAILREFCONF_R, &DbtcProxy::forwardToAnyWorker);

  // GSN_CREATE_FK_IMPL_REQ
  addRecSignal(GSN_CREATE_FK_IMPL_REQ, &DbtcProxy::execCREATE_FK_IMPL_REQ);
  addRecSignal(GSN_CREATE_FK_IMPL_CONF, &DbtcProxy::execCREATE_FK_IMPL_CONF);
  addRecSignal(GSN_CREATE_FK_IMPL_REF, &DbtcProxy::execCREATE_FK_IMPL_REF);

  // GSN_DROP_FK_IMPL_REQ
  addRecSignal(GSN_DROP_FK_IMPL_REQ, &DbtcProxy::execDROP_FK_IMPL_REQ);
  addRecSignal(GSN_DROP_FK_IMPL_CONF, &DbtcProxy::execDROP_FK_IMPL_CONF);
  addRecSignal(GSN_DROP_FK_IMPL_REF, &DbtcProxy::execDROP_FK_IMPL_REF);

  // GSN_CREATE_DB_REQ
  addRecSignal(GSN_CREATE_DB_REQ, &DbtcProxy::execCREATE_DB_REQ);
  addRecSignal(GSN_CREATE_DB_CONF, &DbtcProxy::execCREATE_DB_CONF);
  addRecSignal(GSN_CREATE_DB_REF, &DbtcProxy::execCREATE_DB_REF);

  // GSN_ALTER_DB_REQ
  addRecSignal(GSN_ALTER_DB_REQ, &DbtcProxy::execALTER_DB_REQ);
  addRecSignal(GSN_ALTER_DB_CONF, &DbtcProxy::execALTER_DB_CONF);
  addRecSignal(GSN_ALTER_DB_REF, &DbtcProxy::execALTER_DB_REF);

  // GSN_DROP_DB_REQ
  addRecSignal(GSN_DROP_DB_REQ, &DbtcProxy::execDROP_DB_REQ);
  addRecSignal(GSN_DROP_DB_CONF, &DbtcProxy::execDROP_DB_CONF);
  addRecSignal(GSN_DROP_DB_REF, &DbtcProxy::execDROP_DB_REF);

  // GSN_COMMIT_DB_REQ
  addRecSignal(GSN_COMMIT_DB_REQ, &DbtcProxy::execCOMMIT_DB_REQ);
  addRecSignal(GSN_COMMIT_DB_CONF, &DbtcProxy::execCOMMIT_DB_CONF);
  addRecSignal(GSN_COMMIT_DB_REF, &DbtcProxy::execCOMMIT_DB_REF);

  // GSN_CONNECT_TABLE_DB_REQ
  addRecSignal(GSN_CONNECT_TABLE_DB_REQ,
               &DbtcProxy::execCONNECT_TABLE_DB_REQ);
  addRecSignal(GSN_CONNECT_TABLE_DB_CONF,
               &DbtcProxy::execCONNECT_TABLE_DB_CONF);
  addRecSignal(GSN_CONNECT_TABLE_DB_REF,
               &DbtcProxy::execCONNECT_TABLE_DB_REF);

  // GSN_DISCONNECT_TABLE_DB_REQ
  addRecSignal(GSN_DISCONNECT_TABLE_DB_REQ,
               &DbtcProxy::execDISCONNECT_TABLE_DB_REQ);
  addRecSignal(GSN_DISCONNECT_TABLE_DB_CONF,
               &DbtcProxy::execDISCONNECT_TABLE_DB_CONF);
  addRecSignal(GSN_DISCONNECT_TABLE_DB_REF,
               &DbtcProxy::execDISCONNECT_TABLE_DB_REF);

  addRecSignal(GSN_DATABASE_RATE_ORD, &DbtcProxy::execDATABASE_RATE_ORD);
  addRecSignal(GSN_RATE_OVERLOAD_REP, &DbtcProxy::execRATE_OVERLOAD_REP);
  addRecSignal(GSN_QUOTA_OVERLOAD_REP, &DbtcProxy::execQUOTA_OVERLOAD_REP);

  m_tc_seize_req_instance = 0;
}

DbtcProxy::~DbtcProxy() {}

SimulatedBlock *DbtcProxy::newWorker(Uint32 instanceNo) {
  //  return new (NdbMem_AlignedAlloc(64, sizeof(Dbtc))) Dbtc(m_ctx,
  //  instanceNo);
  return new Dbtc(m_ctx, instanceNo);
}

// GSN_NDB_STTOR

void DbtcProxy::callNDB_STTOR(Signal *signal) {
  jam();
  Ss_READ_NODES_REQ &ss = c_ss_READ_NODESREQ;
  ndbrequire(ss.m_gsn == 0);

  const Uint32 startPhase = signal->theData[2];
  switch (startPhase) {
  case 3:
    jam();
    ss.m_gsn = GSN_NDB_STTOR;
    sendREAD_NODESREQ(signal);
    break;
  default:
    jam();
    backNDB_STTOR(signal);
    break;
  }
}

void DbtcProxy::execTCSEIZEREQ(Signal *signal) {
  jamEntry();

  if (signal->getLength() >= 3 && signal->theData[2] != 0) {
    jam();
    /**
     * Specific instance requested...
     */
    Uint32 instance = signal->theData[2];
    if (instance == 0 || instance > c_workers) {
      jam();
      Uint32 senderData = signal->theData[0];
      Uint32 senderRef = signal->theData[1];
      signal->theData[0] = senderData;
      signal->theData[1] = 289;
      sendSignal(senderRef, GSN_TCSEIZEREF, signal, 2, JBB);
      return;
    }

    sendSignal(workerRef(instance - 1), GSN_TCSEIZEREQ, signal,
               signal->getLength(), JBB);
    return;
  }

  if (globalData.ndbMtTcThreads == 0)
  {
    jam();
    BlockReference sender_ref = signal->senderBlockRef();
    NodeId node_id = refToNode(sender_ref);
    Uint32 instance;
    if (node_id == getOwnNodeId())
    {
      instance = 0; /* Handle Startup */
    }
    else
    {
      instance = map_api_node_to_recv_instance(node_id);
      ndbrequire(instance != RNIL);
      ndbrequire(instance < globalData.ndbMtReceiveThreads);
    }
    signal->theData[2] = (1 + instance);
    sendSignal(workerRef(instance), GSN_TCSEIZEREQ, signal,
               signal->getLength(), JBB);
  }
  else
  {
    jam();
    signal->theData[2] = 1 + m_tc_seize_req_instance;
    sendSignal(workerRef(m_tc_seize_req_instance), GSN_TCSEIZEREQ, signal,
               signal->getLength(), JBB);
    m_tc_seize_req_instance = (m_tc_seize_req_instance + 1) % c_workers;
  }
}

// GSN_TCGETOPSIZEREQ

void DbtcProxy::execTCGETOPSIZEREQ(Signal *signal) {
  jam();
  Ss_TCGETOPSIZEREQ &ss = ssSeize<Ss_TCGETOPSIZEREQ>(1);

  ss.m_sum = 0;
  memcpy(ss.m_req, signal->getDataPtr(), 2 * 4);
  sendREQ(signal, ss);
}

void DbtcProxy::sendTCGETOPSIZEREQ(Signal *signal, Uint32 ssId,
                                   SectionHandle *) {
  jam();
  Ss_TCGETOPSIZEREQ &ss = ssFind<Ss_TCGETOPSIZEREQ>(ssId);

  signal->theData[0] = ssId;
  signal->theData[1] = reference();
  sendSignal(workerRef(ss.m_worker), GSN_TCGETOPSIZEREQ, signal, 2, JBB);
}

void DbtcProxy::execTCGETOPSIZECONF(Signal *signal) {
  jam();
  Uint32 ssId = signal->theData[0];
  Ss_TCGETOPSIZEREQ &ss = ssFind<Ss_TCGETOPSIZEREQ>(ssId);
  ss.m_sum += signal->theData[1];
  recvCONF(signal, ss);
}

void DbtcProxy::sendTCGETOPSIZECONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_TCGETOPSIZEREQ &ss = ssFind<Ss_TCGETOPSIZEREQ>(ssId);

  if (!lastReply(ss)) {
    jam();
    return;
  }

  signal->theData[0] = ss.m_req[0];
  signal->theData[1] = ss.m_sum;
  signal->theData[2] = ss.m_req[1];

  sendSignal(DBDIH_REF, GSN_CHECK_LCP_IDLE_ORD, signal, 3, JBB);
  ssRelease<Ss_TCGETOPSIZEREQ>(ssId);
}

// GSN_TC_CLOPSIZEREQ

void DbtcProxy::execTC_CLOPSIZEREQ(Signal *signal) {
  jam();
  Ss_TC_CLOPSIZEREQ &ss = ssSeize<Ss_TC_CLOPSIZEREQ>(1);

  memcpy(ss.m_req, signal->getDataPtr(), 2 * 4);
  sendREQ(signal, ss);
}

void DbtcProxy::sendTC_CLOPSIZEREQ(Signal *signal, Uint32 ssId,
                                   SectionHandle *) {
  jam();
  Ss_TC_CLOPSIZEREQ &ss = ssFind<Ss_TC_CLOPSIZEREQ>(ssId);

  signal->theData[0] = ssId;
  signal->theData[1] = reference();
  sendSignal(workerRef(ss.m_worker), GSN_TC_CLOPSIZEREQ, signal, 2, JBB);
}

void DbtcProxy::execTC_CLOPSIZECONF(Signal *signal) {
  jam();
  Uint32 ssId = signal->theData[0];
  Ss_TC_CLOPSIZEREQ &ss = ssFind<Ss_TC_CLOPSIZEREQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::sendTC_CLOPSIZECONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_TC_CLOPSIZEREQ &ss = ssFind<Ss_TC_CLOPSIZEREQ>(ssId);

  if (!lastReply(ss)) {
    jam();
    return;
  }

  signal->theData[0] = ss.m_req[0];
  sendSignal(ss.m_req[1], GSN_TC_CLOPSIZECONF, signal, 1, JBB);

  ssRelease<Ss_TC_CLOPSIZEREQ>(ssId);
}

// GSN_GCP_NOMORETRANS

void DbtcProxy::execGCP_NOMORETRANS(Signal *signal) {
  jam();
  Ss_GCP_NOMORETRANS &ss = ssSeize<Ss_GCP_NOMORETRANS>(1);

  ss.m_req = *(GCPNoMoreTrans *)signal->getDataPtr();
  ss.m_minTcFailNo = ~Uint32(0);
  sendREQ(signal, ss);
}

void DbtcProxy::sendGCP_NOMORETRANS(Signal *signal, Uint32 ssId,
                                    SectionHandle *) {
  jam();
  Ss_GCP_NOMORETRANS &ss = ssFind<Ss_GCP_NOMORETRANS>(ssId);

  GCPNoMoreTrans *req = (GCPNoMoreTrans *)signal->getDataPtrSend();
  req->senderRef = reference();
  req->senderData = ssId;
  req->gci_hi = ss.m_req.gci_hi;
  req->gci_lo = ss.m_req.gci_lo;
  sendSignal(workerRef(ss.m_worker), GSN_GCP_NOMORETRANS, signal,
             GCPNoMoreTrans::SignalLength, JBB);
}

void DbtcProxy::execGCP_TCFINISHED(Signal *signal) {
  jam();
  GCPTCFinished *conf = (GCPTCFinished *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_GCP_NOMORETRANS &ss = ssFind<Ss_GCP_NOMORETRANS>(ssId);

  /* Record minimum handled failure number seen from TC workers */
  if (conf->tcFailNo < ss.m_minTcFailNo) {
    jam();
    ss.m_minTcFailNo = conf->tcFailNo;
  }
  
  recvCONF(signal, ss);
}

void DbtcProxy::sendGCP_TCFINISHED(Signal *signal, Uint32 ssId) {
  jam();
  Ss_GCP_NOMORETRANS &ss = ssFind<Ss_GCP_NOMORETRANS>(ssId);

  if (!lastReply(ss)) {
    jam();
    return;
  }

  GCPTCFinished *conf = (GCPTCFinished *)signal->getDataPtrSend();
  conf->senderData = ss.m_req.senderData;
  conf->gci_hi = ss.m_req.gci_hi;
  conf->gci_lo = ss.m_req.gci_lo;
  conf->tcFailNo = ss.m_minTcFailNo;
  sendSignal(ss.m_req.senderRef, GSN_GCP_TCFINISHED, signal,
             GCPTCFinished::SignalLength, JBB);

  ssRelease<Ss_GCP_NOMORETRANS>(ssId);
}

// GSN_CREATE_INDX_IMPL_REQ

void DbtcProxy::execCREATE_INDX_IMPL_REQ(Signal *signal) {
  if (!assembleFragments(signal)) {
    jam();
    return;
  }
  jamEntry();

  const CreateIndxImplReq *req =
      (const CreateIndxImplReq *)signal->getDataPtr();
  Ss_CREATE_INDX_IMPL_REQ &ss = ssSeize<Ss_CREATE_INDX_IMPL_REQ>();
  ss.m_req = *req;
  SectionHandle handle(this, signal);
  saveSections(ss, handle);
  sendREQ(signal, ss);
}

void DbtcProxy::sendCREATE_INDX_IMPL_REQ(Signal *signal, Uint32 ssId,
                                         SectionHandle *handle) {
  jam();
  Ss_CREATE_INDX_IMPL_REQ &ss = ssFind<Ss_CREATE_INDX_IMPL_REQ>(ssId);

  CreateIndxImplReq *req = (CreateIndxImplReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_CREATE_INDX_IMPL_REQ, signal,
                      CreateIndxImplReq::SignalLength, JBB, handle);
}

void DbtcProxy::execCREATE_INDX_IMPL_CONF(Signal *signal) {
  jam();
  const CreateIndxImplConf *conf =
      (const CreateIndxImplConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_CREATE_INDX_IMPL_REQ &ss = ssFind<Ss_CREATE_INDX_IMPL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execCREATE_INDX_IMPL_REF(Signal *signal) {
  jam();
  const CreateIndxImplRef *ref =
      (const CreateIndxImplRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_CREATE_INDX_IMPL_REQ &ss = ssFind<Ss_CREATE_INDX_IMPL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendCREATE_INDX_IMPL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_CREATE_INDX_IMPL_REQ &ss = ssFind<Ss_CREATE_INDX_IMPL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    CreateIndxImplConf *conf = (CreateIndxImplConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_CREATE_INDX_IMPL_CONF, signal,
               CreateIndxImplConf::SignalLength, JBB);
  } else {
    jam();
    CreateIndxImplRef *ref = (CreateIndxImplRef *)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_CREATE_INDX_IMPL_REF, signal,
               CreateIndxImplRef::SignalLength, JBB);
  }

  ssRelease<Ss_CREATE_INDX_IMPL_REQ>(ssId);
}

// GSN_ALTER_INDX_IMPL_REQ

void DbtcProxy::execALTER_INDX_IMPL_REQ(Signal *signal) {
  jam();
  const AlterIndxImplReq *req = (const AlterIndxImplReq *)signal->getDataPtr();
  Ss_ALTER_INDX_IMPL_REQ &ss = ssSeize<Ss_ALTER_INDX_IMPL_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == AlterIndxImplReq::SignalLength);
  sendREQ(signal, ss);
}

void DbtcProxy::sendALTER_INDX_IMPL_REQ(Signal *signal, Uint32 ssId,
                                        SectionHandle *) {
  jam();
  Ss_ALTER_INDX_IMPL_REQ &ss = ssFind<Ss_ALTER_INDX_IMPL_REQ>(ssId);

  AlterIndxImplReq *req = (AlterIndxImplReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignal(workerRef(ss.m_worker), GSN_ALTER_INDX_IMPL_REQ, signal,
             AlterIndxImplReq::SignalLength, JBB);
}

void DbtcProxy::execALTER_INDX_IMPL_CONF(Signal *signal) {
  jam();
  const AlterIndxImplConf *conf =
      (const AlterIndxImplConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_ALTER_INDX_IMPL_REQ &ss = ssFind<Ss_ALTER_INDX_IMPL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execALTER_INDX_IMPL_REF(Signal *signal) {
  jam();
  const AlterIndxImplRef *ref = (const AlterIndxImplRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_ALTER_INDX_IMPL_REQ &ss = ssFind<Ss_ALTER_INDX_IMPL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendALTER_INDX_IMPL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_ALTER_INDX_IMPL_REQ &ss = ssFind<Ss_ALTER_INDX_IMPL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    AlterIndxImplConf *conf = (AlterIndxImplConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_ALTER_INDX_IMPL_CONF, signal,
               AlterIndxImplConf::SignalLength, JBB);
  } else {
    jam();
    AlterIndxImplRef *ref = (AlterIndxImplRef *)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_ALTER_INDX_IMPL_REF, signal,
               AlterIndxImplRef::SignalLength, JBB);
  }

  ssRelease<Ss_ALTER_INDX_IMPL_REQ>(ssId);
}

// GSN_DROP_INDX_IMPL_REQ

void DbtcProxy::execDROP_INDX_IMPL_REQ(Signal *signal) {
  jam();
  const DropIndxImplReq *req = (const DropIndxImplReq *)signal->getDataPtr();
  Ss_DROP_INDX_IMPL_REQ &ss = ssSeize<Ss_DROP_INDX_IMPL_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == DropIndxImplReq::SignalLength);
  sendREQ(signal, ss);
}

void DbtcProxy::sendDROP_INDX_IMPL_REQ(Signal *signal, Uint32 ssId,
                                       SectionHandle *) {
  jam();
  Ss_DROP_INDX_IMPL_REQ &ss = ssFind<Ss_DROP_INDX_IMPL_REQ>(ssId);

  DropIndxImplReq *req = (DropIndxImplReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignal(workerRef(ss.m_worker), GSN_DROP_INDX_IMPL_REQ, signal,
             DropIndxImplReq::SignalLength, JBB);
}

void DbtcProxy::execDROP_INDX_IMPL_CONF(Signal *signal) {
  jam();
  const DropIndxImplConf *conf = (const DropIndxImplConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_DROP_INDX_IMPL_REQ &ss = ssFind<Ss_DROP_INDX_IMPL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execDROP_INDX_IMPL_REF(Signal *signal) {
  jam();
  const DropIndxImplRef *ref = (const DropIndxImplRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_DROP_INDX_IMPL_REQ &ss = ssFind<Ss_DROP_INDX_IMPL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendDROP_INDX_IMPL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_DROP_INDX_IMPL_REQ &ss = ssFind<Ss_DROP_INDX_IMPL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    DropIndxImplConf *conf = (DropIndxImplConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_DROP_INDX_IMPL_CONF, signal,
               DropIndxImplConf::SignalLength, JBB);
  } else {
    jam();
    DropIndxImplRef *ref = (DropIndxImplRef *)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_DROP_INDX_IMPL_REF, signal,
               DropIndxImplRef::SignalLength, JBB);
  }

  ssRelease<Ss_DROP_INDX_IMPL_REQ>(ssId);
}

void DbtcProxy::execTAKE_OVERTCCONF(Signal *signal) {
  jamEntry();

  if (!checkNodeFailSequence(signal)) {
    jam();
    return;
  }

  for (Uint32 i = 0; i < c_workers; i++) {
    jam();
    Uint32 ref = numberToRef(number(), workerInstance(i), getOwnNodeId());
    sendSignal(ref, GSN_TAKE_OVERTCCONF, signal, signal->getLength(), JBB);
  }
}

// GSN_ABORT_ALL_REQ

void DbtcProxy::execABORT_ALL_REQ(Signal *signal) {
  jam();
  const AbortAllReq *req = (const AbortAllReq *)signal->getDataPtr();
  Ss_ABORT_ALL_REQ &ss = ssSeize<Ss_ABORT_ALL_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == AbortAllReq::SignalLength);
  sendREQ(signal, ss);
}

void DbtcProxy::sendABORT_ALL_REQ(Signal *signal, Uint32 ssId,
                                  SectionHandle *) {
  jam();
  Ss_ABORT_ALL_REQ &ss = ssFind<Ss_ABORT_ALL_REQ>(ssId);

  AbortAllReq *req = (AbortAllReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignal(workerRef(ss.m_worker), GSN_ABORT_ALL_REQ, signal,
             AbortAllReq::SignalLength, JBB);
}

void DbtcProxy::execABORT_ALL_CONF(Signal *signal) {
  jam();
  const AbortAllConf *conf = (const AbortAllConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_ABORT_ALL_REQ &ss = ssFind<Ss_ABORT_ALL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execABORT_ALL_REF(Signal *signal) {
  jam();
  const AbortAllRef *ref = (const AbortAllRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_ABORT_ALL_REQ &ss = ssFind<Ss_ABORT_ALL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendABORT_ALL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_ABORT_ALL_REQ &ss = ssFind<Ss_ABORT_ALL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    AbortAllConf *conf = (AbortAllConf *)signal->getDataPtrSend();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_ABORT_ALL_CONF, signal, AbortAllConf::SignalLength,
               JBB);
  } else {
    jam();
    AbortAllRef *ref = (AbortAllRef *)signal->getDataPtrSend();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_ABORT_ALL_REF, signal, AbortAllRef::SignalLength,
               JBB);
  }

  ssRelease<Ss_ABORT_ALL_REQ>(ssId);
}

// GSN_CREATE_FK_IMPL_REQ

void DbtcProxy::execCREATE_FK_IMPL_REQ(Signal *signal) {
  if (!assembleFragments(signal)) {
    jam();
    return;
  }
  jamEntry();

  const CreateFKImplReq *req = (const CreateFKImplReq *)signal->getDataPtr();
  Ss_CREATE_FK_IMPL_REQ &ss = ssSeize<Ss_CREATE_FK_IMPL_REQ>();
  ss.m_req = *req;
  SectionHandle handle(this, signal);
  saveSections(ss, handle);
  sendREQ(signal, ss);
}

void DbtcProxy::sendCREATE_FK_IMPL_REQ(Signal *signal, Uint32 ssId,
                                       SectionHandle *handle) {
  jam();
  Ss_CREATE_FK_IMPL_REQ &ss = ssFind<Ss_CREATE_FK_IMPL_REQ>(ssId);

  CreateFKImplReq *req = (CreateFKImplReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_CREATE_FK_IMPL_REQ, signal,
                      CreateFKImplReq::SignalLength, JBB, handle);
}

void DbtcProxy::execCREATE_FK_IMPL_CONF(Signal *signal) {
  jam();
  const CreateFKImplConf *conf = (const CreateFKImplConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_CREATE_FK_IMPL_REQ &ss = ssFind<Ss_CREATE_FK_IMPL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execCREATE_FK_IMPL_REF(Signal *signal) {
  jam();
  const CreateFKImplRef *ref = (const CreateFKImplRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_CREATE_FK_IMPL_REQ &ss = ssFind<Ss_CREATE_FK_IMPL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendCREATE_FK_IMPL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_CREATE_FK_IMPL_REQ &ss = ssFind<Ss_CREATE_FK_IMPL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    CreateFKImplConf *conf = (CreateFKImplConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_CREATE_FK_IMPL_CONF, signal,
               CreateFKImplConf::SignalLength, JBB);
  } else {
    jam();
    CreateFKImplRef *ref = (CreateFKImplRef *)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_CREATE_FK_IMPL_REF, signal,
               CreateFKImplRef::SignalLength, JBB);
  }

  ssRelease<Ss_CREATE_FK_IMPL_REQ>(ssId);
}

// GSN_DROP_FK_IMPL_REQ

void DbtcProxy::execDROP_FK_IMPL_REQ(Signal *signal) {
  jam();
  const DropFKImplReq *req = (const DropFKImplReq *)signal->getDataPtr();
  Ss_DROP_FK_IMPL_REQ &ss = ssSeize<Ss_DROP_FK_IMPL_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == DropFKImplReq::SignalLength);
  sendREQ(signal, ss);
}

void DbtcProxy::sendDROP_FK_IMPL_REQ(Signal *signal, Uint32 ssId,
                                     SectionHandle *) {
  jam();
  Ss_DROP_FK_IMPL_REQ &ss = ssFind<Ss_DROP_FK_IMPL_REQ>(ssId);

  DropFKImplReq *req = (DropFKImplReq *)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignal(workerRef(ss.m_worker), GSN_DROP_FK_IMPL_REQ, signal,
             DropFKImplReq::SignalLength, JBB);
}

void DbtcProxy::execDROP_FK_IMPL_CONF(Signal *signal) {
  jam();
  const DropFKImplConf *conf = (const DropFKImplConf *)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_DROP_FK_IMPL_REQ &ss = ssFind<Ss_DROP_FK_IMPL_REQ>(ssId);
  recvCONF(signal, ss);
}

void DbtcProxy::execDROP_FK_IMPL_REF(Signal *signal) {
  jam();
  const DropFKImplRef *ref = (const DropFKImplRef *)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_DROP_FK_IMPL_REQ &ss = ssFind<Ss_DROP_FK_IMPL_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void DbtcProxy::sendDROP_FK_IMPL_CONF(Signal *signal, Uint32 ssId) {
  jam();
  Ss_DROP_FK_IMPL_REQ &ss = ssFind<Ss_DROP_FK_IMPL_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss)) {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    DropFKImplConf *conf = (DropFKImplConf *)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    sendSignal(dictRef, GSN_DROP_FK_IMPL_CONF, signal,
               DropFKImplConf::SignalLength, JBB);
  } else {
    jam();
    DropFKImplRef *ref = (DropFKImplRef *)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->errorCode = ss.m_error;
    sendSignal(dictRef, GSN_DROP_FK_IMPL_REF, signal,
               DropFKImplRef::SignalLength, JBB);
  }

  ssRelease<Ss_DROP_FK_IMPL_REQ>(ssId);
}

// GSN_CREATE_DB_REQ

void
DbtcProxy::execCREATE_DB_REQ(Signal* signal)
{
  jam();
  const CreateDbReq* req = (const CreateDbReq*)signal->getDataPtr();
  Ss_CREATE_DB_REQ& ss = ssSeize<Ss_CREATE_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == CreateDbReq::SignalLengthTC);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendCREATE_DB_REQ(Signal* signal,
                             Uint32 ssId,
                             SectionHandle* handle)
{
  jam();
  Ss_CREATE_DB_REQ& ss = ssFind<Ss_CREATE_DB_REQ>(ssId);

  CreateDbReq* req = (CreateDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_CREATE_DB_REQ,
                      signal, CreateDbReq::SignalLengthTC, JBB, handle);
}

void
DbtcProxy::execCREATE_DB_CONF(Signal* signal)
{
  jam();
  const CreateDbConf* conf = (const CreateDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_CREATE_DB_REQ& ss = ssFind<Ss_CREATE_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execCREATE_DB_REF(Signal* signal)
{
  jam();
  const CreateDbRef* ref = (const CreateDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_CREATE_DB_REQ& ss = ssFind<Ss_CREATE_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendCREATE_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_CREATE_DB_REQ& ss = ssFind<Ss_CREATE_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    CreateDbConf* conf = (CreateDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->databaseId = ss.m_req.databaseId;
    sendSignal(dictRef, GSN_CREATE_DB_CONF,
               signal, CreateDbConf::SignalLength, JBB);
  } else {
    jam();
    CreateDbRef* ref = (CreateDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->databaseId = ss.m_req.databaseId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_CREATE_DB_REF,
               signal, CreateDbConf::SignalLength, JBB);
  }

  ssRelease<Ss_CREATE_DB_REQ>(ssId);
}

// GSN_CONNECT_TABLE_DB_REQ

void
DbtcProxy::execCONNECT_TABLE_DB_REQ(Signal* signal)
{
  jam();
  const ConnectTableDbReq* req =
    (const ConnectTableDbReq*)signal->getDataPtr();
  Ss_CONNECT_TABLE_DB_REQ& ss = ssSeize<Ss_CONNECT_TABLE_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == ConnectTableDbReq::SignalLength);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendCONNECT_TABLE_DB_REQ(Signal* signal,
                                    Uint32 ssId,
                                    SectionHandle* handle)
{
  jam();
  Ss_CONNECT_TABLE_DB_REQ& ss = ssFind<Ss_CONNECT_TABLE_DB_REQ>(ssId);

  ConnectTableDbReq* req = (ConnectTableDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_CONNECT_TABLE_DB_REQ,
                      signal, ConnectTableDbReq::SignalLength, JBB, handle);
}

void
DbtcProxy::execCONNECT_TABLE_DB_CONF(Signal* signal)
{
  jam();
  const ConnectTableDbConf* conf =
    (const ConnectTableDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_CONNECT_TABLE_DB_REQ& ss = ssFind<Ss_CONNECT_TABLE_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execCONNECT_TABLE_DB_REF(Signal* signal)
{
  jam();
  const ConnectTableDbRef* ref =
    (const ConnectTableDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_CONNECT_TABLE_DB_REQ& ss = ssFind<Ss_CONNECT_TABLE_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendCONNECT_TABLE_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_CONNECT_TABLE_DB_REQ& ss = ssFind<Ss_CONNECT_TABLE_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    ConnectTableDbConf* conf = (ConnectTableDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->requestType = ss.m_req.requestType;
    conf->databaseId = ss.m_req.databaseId;
    conf->tableId = ss.m_req.tableId;
    sendSignal(dictRef, GSN_CONNECT_TABLE_DB_CONF,
               signal, ConnectTableDbConf::SignalLength, JBB);
  } else {
    jam();
    ConnectTableDbRef* ref = (ConnectTableDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->requestType = ss.m_req.requestType;
    ref->databaseId = ss.m_req.databaseId;
    ref->tableId = ss.m_req.tableId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_CONNECT_TABLE_DB_REF,
               signal, ConnectTableDbConf::SignalLength, JBB);
  }
  ssRelease<Ss_CONNECT_TABLE_DB_REQ>(ssId);
}

// GSN_DISCONNECT_TABLE_DB_REQ

void
DbtcProxy::execDISCONNECT_TABLE_DB_REQ(Signal* signal)
{
  jam();
  const DisconnectTableDbReq* req =
    (const DisconnectTableDbReq*)signal->getDataPtr();
  Ss_DISCONNECT_TABLE_DB_REQ& ss = ssSeize<Ss_DISCONNECT_TABLE_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == DisconnectTableDbReq::SignalLength);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendDISCONNECT_TABLE_DB_REQ(Signal* signal,
                                       Uint32 ssId,
                                       SectionHandle* handle)
{
  jam();
  Ss_DISCONNECT_TABLE_DB_REQ& ss = ssFind<Ss_DISCONNECT_TABLE_DB_REQ>(ssId);

  DisconnectTableDbReq* req = (DisconnectTableDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_DISCONNECT_TABLE_DB_REQ,
                      signal, DisconnectTableDbReq::SignalLength, JBB, handle);
}

void
DbtcProxy::execDISCONNECT_TABLE_DB_CONF(Signal* signal)
{
  jam();
  const DisconnectTableDbConf* conf =
    (const DisconnectTableDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_DISCONNECT_TABLE_DB_REQ& ss = ssFind<Ss_DISCONNECT_TABLE_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execDISCONNECT_TABLE_DB_REF(Signal* signal)
{
  jam();
  const DisconnectTableDbRef* ref =
    (const DisconnectTableDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_DISCONNECT_TABLE_DB_REQ& ss = ssFind<Ss_DISCONNECT_TABLE_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendDISCONNECT_TABLE_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_DISCONNECT_TABLE_DB_REQ& ss = ssFind<Ss_DISCONNECT_TABLE_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    DisconnectTableDbConf* conf = (DisconnectTableDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->requestType = ss.m_req.requestType;
    conf->databaseId = ss.m_req.databaseId;
    conf->tableId = ss.m_req.tableId;
    sendSignal(dictRef, GSN_DISCONNECT_TABLE_DB_CONF,
               signal, DisconnectTableDbConf::SignalLength, JBB);
  } else {
    jam();
    DisconnectTableDbRef* ref = (DisconnectTableDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->requestType = ss.m_req.requestType;
    ref->databaseId = ss.m_req.databaseId;
    ref->tableId = ss.m_req.tableId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_DISCONNECT_TABLE_DB_REF,
               signal, DisconnectTableDbConf::SignalLength, JBB);
  }
  ssRelease<Ss_DISCONNECT_TABLE_DB_REQ>(ssId);
}

// GSN_COMMIT_DB_REQ

void
DbtcProxy::execCOMMIT_DB_REQ(Signal* signal)
{
  jam();
  const CommitDbReq* req =
    (const CommitDbReq*)signal->getDataPtr();
  Ss_COMMIT_DB_REQ& ss = ssSeize<Ss_COMMIT_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == CommitDbReq::SignalLength);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendCOMMIT_DB_REQ(Signal* signal,
                             Uint32 ssId,
                             SectionHandle* handle)
{
  jam();
  Ss_COMMIT_DB_REQ& ss = ssFind<Ss_COMMIT_DB_REQ>(ssId);

  CommitDbReq* req = (CommitDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_COMMIT_DB_REQ,
                      signal, CommitDbReq::SignalLength, JBB, handle);
}

void
DbtcProxy::execCOMMIT_DB_CONF(Signal* signal)
{
  jam();
  const CommitDbConf* conf =
    (const CommitDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_COMMIT_DB_REQ& ss = ssFind<Ss_COMMIT_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execCOMMIT_DB_REF(Signal* signal)
{
  jam();
  const CommitDbRef* ref =
    (const CommitDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_COMMIT_DB_REQ& ss = ssFind<Ss_COMMIT_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendCOMMIT_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_COMMIT_DB_REQ& ss = ssFind<Ss_COMMIT_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    CommitDbConf* conf = (CommitDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->databaseId = ss.m_req.databaseId;
    sendSignal(dictRef, GSN_COMMIT_DB_CONF,
               signal, CommitDbConf::SignalLength, JBB);
  } else {
    jam();
    CommitDbRef* ref = (CommitDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->databaseId = ss.m_req.databaseId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_COMMIT_DB_REF,
               signal, CommitDbConf::SignalLength, JBB);
  }
  ssRelease<Ss_COMMIT_DB_REQ>(ssId);
}

// GSN_DROP_DB_REQ

void
DbtcProxy::execDROP_DB_REQ(Signal* signal)
{
  jam();
  const DropDbReq* req =
    (const DropDbReq*)signal->getDataPtr();
  Ss_DROP_DB_REQ& ss = ssSeize<Ss_DROP_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == DropDbReq::SignalLength);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendDROP_DB_REQ(Signal* signal,
                           Uint32 ssId,
                           SectionHandle* handle)
{
  jam();
  Ss_DROP_DB_REQ& ss = ssFind<Ss_DROP_DB_REQ>(ssId);

  DropDbReq* req = (DropDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_DROP_DB_REQ,
                      signal, DropDbReq::SignalLength, JBB, handle);
}

void
DbtcProxy::execDROP_DB_CONF(Signal* signal)
{
  jam();
  const DropDbConf* conf =
    (const DropDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_DROP_DB_REQ& ss = ssFind<Ss_DROP_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execDROP_DB_REF(Signal* signal)
{
  jam();
  const DropDbRef* ref =
    (const DropDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_DROP_DB_REQ& ss = ssFind<Ss_DROP_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendDROP_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_DROP_DB_REQ& ss = ssFind<Ss_DROP_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    DropDbConf* conf = (DropDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->databaseId = ss.m_req.databaseId;
    sendSignal(dictRef, GSN_DROP_DB_CONF,
               signal, DropDbConf::SignalLength, JBB);
  } else {
    jam();
    DropDbRef* ref = (DropDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->databaseId = ss.m_req.databaseId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_DROP_DB_REF,
               signal, DropDbConf::SignalLength, JBB);
  }
  ssRelease<Ss_DROP_DB_REQ>(ssId);
}

// GSN_ALTER_DB_REQ

void
DbtcProxy::execALTER_DB_REQ(Signal* signal)
{
  jam();
  const AlterDbReq* req =
    (const AlterDbReq*)signal->getDataPtr();
  Ss_ALTER_DB_REQ& ss = ssSeize<Ss_ALTER_DB_REQ>();
  ss.m_req = *req;
  ndbrequire(signal->getLength() == AlterDbReq::SignalLength);
  sendREQ(signal, ss);
}

void
DbtcProxy::sendALTER_DB_REQ(Signal* signal,
                            Uint32 ssId,
                            SectionHandle* handle)
{
  jam();
  Ss_ALTER_DB_REQ& ss = ssFind<Ss_ALTER_DB_REQ>(ssId);

  AlterDbReq* req = (AlterDbReq*)signal->getDataPtrSend();
  *req = ss.m_req;
  req->senderRef = reference();
  req->senderData = ssId;
  sendSignalNoRelease(workerRef(ss.m_worker), GSN_ALTER_DB_REQ,
                      signal, AlterDbReq::SignalLength, JBB, handle);
}

void
DbtcProxy::execALTER_DB_CONF(Signal* signal)
{
  jam();
  const AlterDbConf* conf =
    (const AlterDbConf*)signal->getDataPtr();
  Uint32 ssId = conf->senderData;
  Ss_ALTER_DB_REQ& ss = ssFind<Ss_ALTER_DB_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbtcProxy::execALTER_DB_REF(Signal* signal)
{
  jam();
  const AlterDbRef* ref =
    (const AlterDbRef*)signal->getDataPtr();
  Uint32 ssId = ref->senderData;
  Ss_ALTER_DB_REQ& ss = ssFind<Ss_ALTER_DB_REQ>(ssId);
  recvREF(signal, ss, ref->errorCode);
}

void
DbtcProxy::sendALTER_DB_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_ALTER_DB_REQ& ss = ssFind<Ss_ALTER_DB_REQ>(ssId);
  BlockReference dictRef = ss.m_req.senderRef;

  if (!lastReply(ss))
  {
    jam();
    return;
  }

  if (ss.m_error == 0) {
    jam();
    AlterDbConf* conf = (AlterDbConf*)signal->getDataPtrSend();
    conf->senderRef = reference();
    conf->senderData = ss.m_req.senderData;
    conf->databaseId = ss.m_req.databaseId;
    sendSignal(dictRef, GSN_ALTER_DB_CONF,
               signal, AlterDbConf::SignalLength, JBB);
  } else {
    jam();
    AlterDbRef* ref = (AlterDbRef*)signal->getDataPtrSend();
    ref->senderRef = reference();
    ref->senderData = ss.m_req.senderData;
    ref->databaseId = ss.m_req.databaseId;
    ref->errorCode = ss.m_error;
    ref->errorLine = 0;
    ref->errorKey = 0;
    ref->errorStatus = 0;
    sendSignal(dictRef, GSN_ALTER_DB_REF,
               signal, AlterDbConf::SignalLength, JBB);
  }
  ssRelease<Ss_ALTER_DB_REQ>(ssId);
}

void
DbtcProxy::execDATABASE_RATE_ORD(Signal* signal)
{
  /* Distribute signal to all DBTC instances */
  for (Uint32 i = 0; i < globalData.ndbMtTcWorkers; i++)
  {
    BlockReference ref = numberToRef(DBTC, i + 1, getOwnNodeId());
    sendSignal(ref, GSN_DATABASE_RATE_ORD, signal,
               DatabaseRateOrd::SignalLength, JBB);
  }
}

void
DbtcProxy::execRATE_OVERLOAD_REP(Signal* signal)
{
  RateOverloadRep* const rep = (RateOverloadRep*)signal->getDataPtr();
  Uint32 databaseId = rep->databaseId;
  Uint32 num_tc_threads = globalData.ndbMtTcWorkers;
  ndbrequire(num_tc_threads > 0);
  Uint32 instance_tc = (databaseId % num_tc_threads) + 1;
  /* Distribute signal only to global DBTC instance */
  BlockReference ref = numberToRef(DBTC,
                                   instance_tc,
                                   getOwnNodeId());
  sendSignal(ref, GSN_RATE_OVERLOAD_REP, signal,
             RateOverloadRep::SignalLength, JBB);
}

void
DbtcProxy::execQUOTA_OVERLOAD_REP(Signal* signal)
{
  /* Distribute signal to all DBTC instances */
  for (Uint32 i = 0; i < globalData.ndbMtTcWorkers; i++)
  {
    BlockReference ref = numberToRef(DBTC, i + 1, getOwnNodeId());
    sendSignal(ref, GSN_QUOTA_OVERLOAD_REP, signal,
               QuotaOverloadRep::SignalLength, JBB);
  }
}

BLOCK_FUNCTIONS(DbtcProxy)
