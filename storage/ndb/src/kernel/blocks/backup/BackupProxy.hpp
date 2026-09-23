/* Copyright (c) 2008, 2025, Oracle and/or its affiliates.

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

#ifndef NDB_BACKUP_PROXY_HPP
#define NDB_BACKUP_PROXY_HPP

#include <LocalProxy.hpp>
#include <signaldata/BackupImpl.hpp>
#include <signaldata/BackupSignalData.hpp>
#include <signaldata/UtilSequence.hpp>

#define JAM_FILE_ID 478

class BackupProxy : public LocalProxy {
 public:
  BackupProxy(Block_context &ctx);
  ~BackupProxy() override;
  BLOCK_DEFINES(BackupProxy);

 protected:
  SimulatedBlock *newWorker(Uint32 instanceNo) override;

  // GSN_STTOR
  void callSTTOR(Signal *) override;
  void sendUTIL_SEQUENCE_REQ(Signal *);
  void execUTIL_SEQUENCE_CONF(Signal *);
  void execUTIL_SEQUENCE_REF(Signal *);

  struct Ss_SUM_DUMP_STATE_ORD : SsParallel {
    static const int MAX_REQ_SIZE = 2;
    static const int MAX_REP_SIZE = 11;
    Uint32 m_request[MAX_REQ_SIZE];
    Uint32 m_report[MAX_REP_SIZE];

    Ss_SUM_DUMP_STATE_ORD() {
      m_sendREQ = (SsFUNCREQ)&BackupProxy::sendSUM_DUMP_STATE_ORD;
      m_sendCONF = (SsFUNCREP)&BackupProxy::sendSUM_EVENT_REP;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_SUM_DUMP_STATE_ORD> &pool(LocalProxy *proxy) {
      return ((BackupProxy *)proxy)->c_ss_SUM_DUMP_STATE_ORD;
    }
  };
  SsPool<Ss_SUM_DUMP_STATE_ORD> c_ss_SUM_DUMP_STATE_ORD;

  // DUMP_STATE_ORD
  void execDUMP_STATE_ORD(Signal *);
  void sendSUM_DUMP_STATE_ORD(Signal *, Uint32 ssId, SectionHandle *);
  void execEVENT_REP(Signal *);
  void sendSUM_EVENT_REP(Signal *, Uint32 ssId);
  void execRESTORABLE_GCI_REP(Signal *);

  // DEFINE_BACKUP_REQ
  struct Ss_DEFINE_BACKUP_REQ : SsParallel {
    DefineBackupReq m_req;

    Ss_DEFINE_BACKUP_REQ() {
      m_sendREQ = (SsFUNCREQ)&BackupProxy::sendDEFINE_BACKUP_REQ;
      m_sendCONF = (SsFUNCREP)&BackupProxy::sendDEFINE_BACKUP_CONF;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_DEFINE_BACKUP_REQ> &pool(LocalProxy *proxy) {
      return ((BackupProxy *)proxy)->c_ss_DEFINE_BACKUP_REQ;
    }
    Uint32 masterRef;
  };
  SsPool<Ss_DEFINE_BACKUP_REQ> c_ss_DEFINE_BACKUP_REQ;

  /**
   * Node-wide barrier for the failed-backup debris sweep: while LDM1
   * removes the files of a failed backup (RemoveFailedBackupFiles),
   * no DEFINE for that backup id may reach ANY worker on this node -
   * otherwise the new attempt's freshly opened files race the
   * removals. Set when the order passes through here on its way to
   * LDM1, lifted by LDM1's confirm/ref passing back to the master.
   * A zero backup id means no barrier.
   */
  Uint32 m_sweep_backup_id = 0;
  Uint32 m_sweep_gen = 0;
  Uint32 m_sweep_master_ref = 0;
  /**
   * Set once any backup DEFINE has been fanned to the workers this
   * boot, never cleared: from then on every debris sweep order is
   * declined. A fanned attempt's files - or the unacknowledged
   * cleanup tail of its failure (an aggregated REF starts the
   * master's abort, and the workers' file cleanup completes with no
   * signal the proxy could observe; parallel backup records make
   * per-id tracking insufficient) - may be live at any later point,
   * and the removals of a sweep must never overlap them. Costs
   * nothing in the intended flow: sweep orders arrive moments after
   * this node restarted, before any backup could define here, and a
   * declined order is re-ordered at the node's next restart, where
   * this flag is fresh.
   */
  bool m_backup_defined_since_start = false;

  void execDEFINE_BACKUP_REQ(Signal *);
  void sendDEFINE_BACKUP_REQ(Signal *, Uint32 ssId, SectionHandle *);
  void execDEFINE_BACKUP_CONF(Signal *);
  void execDEFINE_BACKUP_REF(Signal *);
  void sendDEFINE_BACKUP_CONF(Signal *, Uint32 ssId);

  // START_BACKUP_REQ
  struct Ss_START_BACKUP_REQ : SsParallel {
    StartBackupReq m_req;

    Ss_START_BACKUP_REQ() {
      m_sendREQ = (SsFUNCREQ)&BackupProxy::sendSTART_BACKUP_REQ;
      m_sendCONF = (SsFUNCREP)&BackupProxy::sendSTART_BACKUP_CONF;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_START_BACKUP_REQ> &pool(LocalProxy *proxy) {
      return ((BackupProxy *)proxy)->c_ss_START_BACKUP_REQ;
    }
    Uint32 masterRef;
  };
  SsPool<Ss_START_BACKUP_REQ> c_ss_START_BACKUP_REQ;
  void execSTART_BACKUP_REQ(Signal *);
  void sendSTART_BACKUP_REQ(Signal *, Uint32 ssId, SectionHandle *);
  void execSTART_BACKUP_CONF(Signal *);
  void execSTART_BACKUP_REF(Signal *);
  void sendSTART_BACKUP_CONF(Signal *, Uint32 ssId);
  // STOP_BACKUP_REQ
  struct Ss_STOP_BACKUP_REQ : SsParallel {
    StopBackupReq m_req;

    Ss_STOP_BACKUP_REQ() {
      m_sendREQ = (SsFUNCREQ)&BackupProxy::sendSTOP_BACKUP_REQ;
      m_sendCONF = (SsFUNCREP)&BackupProxy::sendSTOP_BACKUP_CONF;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_STOP_BACKUP_REQ> &pool(LocalProxy *proxy) {
      return ((BackupProxy *)proxy)->c_ss_STOP_BACKUP_REQ;
    }
    Uint32 masterRef;
  };
  SsPool<Ss_STOP_BACKUP_REQ> c_ss_STOP_BACKUP_REQ;
  void execSTOP_BACKUP_REQ(Signal *);
  void sendSTOP_BACKUP_REQ(Signal *, Uint32 ssId, SectionHandle *);
  void execSTOP_BACKUP_CONF(Signal *);
  void execSTOP_BACKUP_REF(Signal *);
  void sendSTOP_BACKUP_CONF(Signal *, Uint32 ssId);

  // ABORT_BACKUP_ORD
  struct Ss_ABORT_BACKUP_ORD : SsParallel {
    AbortBackupOrd m_req;

    Ss_ABORT_BACKUP_ORD() {
      m_sendREQ = (SsFUNCREQ)&BackupProxy::sendABORT_BACKUP_ORD;
      m_sendCONF = (SsFUNCREP)0;
    }
    enum { poolSize = 1 };
    static SsPool<Ss_ABORT_BACKUP_ORD> &pool(LocalProxy *proxy) {
      return ((BackupProxy *)proxy)->c_ss_ABORT_BACKUP_ORD;
    }
    Uint32 masterRef;
  };
  SsPool<Ss_ABORT_BACKUP_ORD> c_ss_ABORT_BACKUP_ORD;
  void execABORT_BACKUP_ORD(Signal *);
  void sendABORT_BACKUP_ORD(Signal *, Uint32 ssId, SectionHandle *);
  void execNODE_START_REP(Signal *signal);
};

#undef JAM_FILE_ID

#endif
