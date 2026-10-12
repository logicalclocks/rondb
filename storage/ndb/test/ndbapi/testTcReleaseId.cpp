/*
   Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.

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

/**
 * The release id of TCSEIZEREQ and TCRELEASEREQ (ndbd_support_tc_release_id).
 *
 * Seizes TC connect records on every data node with raw signals, and
 * checks that DBTC releases a record only with the release id it was
 * seized with: another id, or none, is ignored (no reply), a release of a
 * record released already is refused with TCRELEASEREF, and a record
 * seized without an id is released as before, only without one.
 */

#include <GlobalSignalNumbers.h>
#include <NdbTick.h>
#include <ndb_version.h>
#include <NDBT.hpp>
#include <NDBT_Test.hpp>
#include <NdbRestarter.hpp>
#include <cstring>
#include "../../src/ndbapi/SignalSender.hpp"

namespace {

/* How long to wait before taking a request as ignored */
constexpr Uint32 IgnoredMillis = 2000;
constexpr Uint32 ReplyMillis = 30000;

class TcReleaseChecker {
 public:
  explicit TcReleaseChecker(SignalSender *ss) : m_ss(ss) {}

  int runTest(Uint32 nodeId) {
    m_nodeId = nodeId;
    if (!checkWithReleaseId() || !checkWithoutReleaseId()) {
      return NDBT_FAILED;
    }
    return NDBT_OK;
  }

 private:
  SignalSender *const m_ss;
  Uint32 m_nodeId{0};
  Uint32 m_userPointer{0};

  /**
   * Wait up to timeoutMillis for gsn1 or gsn2 from the data node with
   * userPointer in word 0.  Returns the GSN, 0 if none came.
   */
  Uint32 waitReply(Uint32 gsn1, Uint32 gsn2, Uint32 userPointer,
                   Uint32 timeoutMillis, Uint32 *data) {
    const NDB_TICKS start = NdbTick_getCurrentTicks();
    while (true) {
      const Uint64 waited =
          NdbTick_Elapsed(start, NdbTick_getCurrentTicks()).milliSec();
      if (waited >= timeoutMillis) {
        return 0;
      }
      const SimpleSignal *signal =
          m_ss->waitFor(Uint32(timeoutMillis - waited));
      if (signal == nullptr) {
        return 0;
      }
      const Uint32 gsn = signal->readSignalNumber();
      if ((gsn == gsn1 || gsn == gsn2) &&
          refToNode(signal->header.theSendersBlockRef) == m_nodeId &&
          signal->getLength() >= 1 &&
          signal->getDataPtr()[0] == userPointer) {
        memcpy(data, signal->getDataPtr(), 4 * signal->getLength());
        return gsn;
      }
    }
  }

  /* TCSEIZEREQ, with releaseId unless it is 0 */
  bool seize(Uint32 releaseId, Uint32 &tcConnectPtr, Uint32 &tcRef) {
    const Uint32 apiConnectPtr = ++m_userPointer;
    SimpleSignal request(false);
    Uint32 *data = request.getDataPtrSend();
    data[0] = apiConnectPtr;
    data[1] = m_ss->getOwnRef();
    data[2] = 0;  // any TC instance
    data[3] = releaseId;
    Uint32 reply[25];
    m_ss->lock();
    const bool sent =
        m_ss->sendSignal(m_nodeId, request, DBTC, GSN_TCSEIZEREQ,
                         releaseId != 0 ? 4 : 3) == SEND_OK;
    const Uint32 gsn = sent ? waitReply(GSN_TCSEIZECONF, GSN_TCSEIZEREF,
                                        apiConnectPtr, ReplyMillis, reply)
                            : 0;
    m_ss->unlock();
    if (gsn != GSN_TCSEIZECONF) {
      g_err << "Node " << m_nodeId << ": TCSEIZEREQ got GSN " << gsn
            << (gsn == GSN_TCSEIZEREF ? " error " : "")
            << (gsn == GSN_TCSEIZEREF ? reply[1] : 0) << endl;
      return false;
    }
    tcConnectPtr = reply[1];
    tcRef = reply[2];
    return true;
  }

  /**
   * TCRELEASEREQ, with releaseId if withId.  Returns the GSN of the
   * reply, 0 if none came within IgnoredMillis.
   */
  Uint32 release(Uint32 tcConnectPtr, Uint32 tcRef, bool withId,
                 Uint32 releaseId) {
    const Uint32 userPointer = ++m_userPointer;
    SimpleSignal request(false);
    Uint32 *data = request.getDataPtrSend();
    data[0] = tcConnectPtr;
    data[1] = m_ss->getOwnRef();
    data[2] = userPointer;
    data[3] = releaseId;
    Uint32 reply[25];
    m_ss->lock();
    const bool sent =
        m_ss->sendSignal(m_nodeId, request, refToBlock(tcRef),
                         GSN_TCRELEASEREQ, withId ? 4 : 3) == SEND_OK;
    const Uint32 gsn = sent ? waitReply(GSN_TCRELEASECONF, GSN_TCRELEASEREF,
                                        userPointer, IgnoredMillis, reply)
                            : 0;
    m_ss->unlock();
    return gsn;
  }

  bool expect(Uint32 gsn, Uint32 expected, const char *what) {
    if (gsn != expected) {
      g_err << "Node " << m_nodeId << ", " << what << ": expected "
            << (expected == 0 ? "no reply" : "GSN ") << expected
            << ", got GSN " << gsn << endl;
      return false;
    }
    return true;
  }

  bool checkWithReleaseId() {
    const Uint32 releaseId = 0x5eed0001u + m_nodeId;
    Uint32 tcConnectPtr = 0;
    Uint32 tcRef = 0;
    if (!seize(releaseId, tcConnectPtr, tcRef)) return false;
    if (!expect(release(tcConnectPtr, tcRef, true, releaseId + 1), 0,
                "release with another id") ||
        !expect(release(tcConnectPtr, tcRef, false, 0), 0,
                "release without an id") ||
        !expect(release(tcConnectPtr, tcRef, true, releaseId),
                GSN_TCRELEASECONF, "release with its id") ||
        !expect(release(tcConnectPtr, tcRef, true, releaseId),
                GSN_TCRELEASEREF, "second release with its id") ||
        !expect(release(tcConnectPtr, tcRef, true, releaseId ^ 0xa5a5a5a5u),
                GSN_TCRELEASEREF,
                "release of the released record with another id")) {
      return false;
    }
    g_info << "Node " << m_nodeId << ": release id checked" << endl;
    return true;
  }

  /* A record seized without an id is released only without one */
  bool checkWithoutReleaseId() {
    Uint32 tcConnectPtr = 0;
    Uint32 tcRef = 0;
    if (!seize(0, tcConnectPtr, tcRef)) return false;
    if (!expect(release(tcConnectPtr, tcRef, true, 0x5eed0100u), 0,
                "release with an id of a record seized without one") ||
        !expect(release(tcConnectPtr, tcRef, false, 0), GSN_TCRELEASECONF,
                "release without an id")) {
      return false;
    }
    g_info << "Node " << m_nodeId << ": release without id checked" << endl;
    return true;
  }
};

}  // namespace

static int runTcReleaseId(NDBT_Context *ctx, NDBT_Step *step) {
  Ndb *pNdb = GETNDB(step);
  SignalSender ss(&pNdb->get_ndb_cluster_connection());
  TcReleaseChecker checker(&ss);
  NdbRestarter restarter;

  const int numDbNodes = restarter.getNumDbNodes();
  if (numDbNodes <= 0) {
    g_err << "No data nodes found" << endl;
    return NDBT_FAILED;
  }
  int nodesChecked = 0;
  for (int i = 0; i < numDbNodes; i++) {
    const Uint32 nodeId = restarter.getDbNodeId(i);
    ss.lock();
    const bool alive = ss.get_node_alive(nodeId);
    const Uint32 version = ss.getNodeInfo(nodeId).m_info.m_version;
    ss.unlock();
    if (!alive) {
      g_err << "Data node " << nodeId << " is not alive" << endl;
      return NDBT_FAILED;
    }
    if (!ndbd_support_tc_release_id(version)) {
      g_info << "Data node " << nodeId << " has version " << hex << version
             << dec << " without a release id, skipped" << endl;
      continue;
    }
    if (checker.runTest(nodeId) != NDBT_OK) return NDBT_FAILED;
    nodesChecked++;
  }
  return nodesChecked > 0 ? NDBT_OK : NDBT_SKIPPED;
}

NDBT_TESTSUITE(testTcReleaseId);
TESTCASE("TcReleaseId",
         "Check that DBTC releases a TC connect record only with the "
         "release id of its TCSEIZEREQ, ignores other ids and refuses a "
         "second release") {
  STEP(runTcReleaseId);
}
NDBT_TESTSUITE_END(testTcReleaseId)

int main(int argc, const char **argv) {
  ndb_init();
  NDBT_TESTSUITE_INSTANCE(testTcReleaseId);
  testTcReleaseId.setCreateTable(false);
  testTcReleaseId.setRunAllTables(true);
  return testTcReleaseId.execute(argc, argv);
}
