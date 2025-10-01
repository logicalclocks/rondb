/* Copyright (c) 2003, 2025, Oracle and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef NDB_DBSPJ_PROXY_HPP
#define NDB_DBSPJ_PROXY_HPP

#include "../dbgdm/DbgdmProxy.hpp"
#include <signaldata/SetDomainId.hpp>

#define JAM_FILE_ID 480

class DbspjProxy : public DbgdmProxy {
 public:
  DbspjProxy(Block_context &ctx);
  ~DbspjProxy() override;
  BLOCK_DEFINES(DbspjProxy);

protected:
  SimulatedBlock *newWorker(Uint32 instanceNo) override;

  /**
   * SET_DOMAIN_ID_REQ 
   */
  struct Ss_SET_DOMAIN_ID_REQ : SsParallel {
    SetDomainIdReq m_req;
    Ss_SET_DOMAIN_ID_REQ() {
      m_sendREQ = (SsFUNCREQ)&DbspjProxy::sendSET_DOMAIN_ID_REQ;
      m_sendCONF = (SsFUNCREP)&DbspjProxy::sendSET_DOMAIN_ID_CONF;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_SET_DOMAIN_ID_REQ>& pool(LocalProxy* proxy) {
      return ((DbspjProxy*)proxy)->c_ss_SET_DOMAIN_ID_REQ;
    }
  };
  SsPool<Ss_SET_DOMAIN_ID_REQ> c_ss_SET_DOMAIN_ID_REQ;
  void execSET_DOMAIN_ID_REQ(Signal*);
  void sendSET_DOMAIN_ID_REQ(Signal*, Uint32 ssId, SectionHandle*);
  void execSET_DOMAIN_ID_CONF(Signal*);
  void sendSET_DOMAIN_ID_CONF(Signal*, Uint32 ssId);
};

#undef JAM_FILE_ID
#endif
