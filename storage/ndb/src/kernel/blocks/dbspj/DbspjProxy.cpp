/*
   Copyright (c) 2000, 2025, Oracle and/or its affiliates.

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

#include "DbspjProxy.hpp"
#include "Dbspj.hpp"

#define JAM_FILE_ID 483

DbspjProxy::DbspjProxy(Block_context &ctx) : DbgdmProxy(DBSPJ, ctx) {
  // GSN_SET_DOMAIN_ID_REQ
  addRecSignal(GSN_SET_DOMAIN_ID_REQ, &DbspjProxy::execSET_DOMAIN_ID_REQ);
  addRecSignal(GSN_SET_DOMAIN_ID_CONF, &DbspjProxy::execSET_DOMAIN_ID_CONF);
}

DbspjProxy::~DbspjProxy() {}

SimulatedBlock *DbspjProxy::newWorker(Uint32 instanceNo) {
  return new Dbspj(m_ctx, instanceNo);
}

// GSN_SET_DOMAIN_ID_REQ

void
DbspjProxy::execSET_DOMAIN_ID_REQ(Signal* signal)
{
  jam();
  Ss_SET_DOMAIN_ID_REQ& ss = ssSeize<Ss_SET_DOMAIN_ID_REQ>(1);
  memcpy(&ss.m_req,
         signal->getDataPtr(),
         sizeof(SetDomainIdReq));
  sendREQ(signal, ss);
}

void
DbspjProxy::sendSET_DOMAIN_ID_REQ(Signal* signal, Uint32 ssId, SectionHandle*)
{
  SetDomainIdReq* const req = (SetDomainIdReq*)signal->getDataPtrSend();
  jam();
  Ss_SET_DOMAIN_ID_REQ& ss = ssFind<Ss_SET_DOMAIN_ID_REQ>(ssId);

  req->senderId = ssId;
  req->senderRef = reference();
  req->changeNodeId = ss.m_req.changeNodeId;
  req->locationDomainId = ss.m_req.locationDomainId;
  sendSignal(workerRef(ss.m_worker),
             GSN_SET_DOMAIN_ID_REQ,
             signal,
             SetDomainIdReq::SignalLength,
             JBB);
}

void
DbspjProxy::execSET_DOMAIN_ID_CONF(Signal* signal)
{
  SetDomainIdConf* const conf = (SetDomainIdConf*)signal->getDataPtr();
  jam();
  Uint32 ssId = conf->senderId;
  Ss_SET_DOMAIN_ID_REQ& ss = ssFind<Ss_SET_DOMAIN_ID_REQ>(ssId);
  recvCONF(signal, ss);
}

void
DbspjProxy::sendSET_DOMAIN_ID_CONF(Signal* signal, Uint32 ssId)
{
  jam();
  Ss_SET_DOMAIN_ID_REQ& ss = ssFind<Ss_SET_DOMAIN_ID_REQ>(ssId);

  if (!lastReply(ss))
  {
    jam();
    return;
  }
  SetDomainIdConf* const conf = (SetDomainIdConf*)signal->getDataPtrSend();
  conf->senderId = ss.m_req.senderId;
  conf->senderRef = reference();
  conf->changeNodeId = ss.m_req.changeNodeId;
  conf->locationDomainId = ss.m_req.locationDomainId;
  sendSignal(QMGR_REF,
             GSN_SET_DOMAIN_ID_CONF,
             signal,
             SetDomainIdConf::SignalLength,
             JBB);
  ssRelease<Ss_SET_DOMAIN_ID_REQ>(ssId);
}
BLOCK_FUNCTIONS(DbspjProxy)
