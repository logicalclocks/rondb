/*
 * Copyright (C) 2026 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */

/*
 * Probe-listener tests that need a live cluster (RDRS_CONFIG_FILE, like the
 * other C++ tests here). They prove the two probe-endpoint guarantees the
 * cluster-free HTTP tests cannot:
 *
 * 1. The startup contract: /health is 503 before the pool exists, stays 503
 *    with a healthy pool until Drogon is up (the g_drogon_up guard - a pod
 *    must never be Ready before the main port accepts), and flips to 200
 *    only when both hold.
 * 2. The never-blocks contract under contention: with threads hammering the
 *    connection-acquisition path (whose critical section creates Ndb
 *    objects while holding both connection mutexes), /health on the probe
 *    port answers fast AND correct - the wait-free stats reads plus the
 *    bounded try-lock, measured end to end over HTTP.
 */

#include "probe_server.hpp"
#include "config_structs.hpp"
#include "connection.hpp"
#include "constants.hpp"
#include "rdrs_dal.h"
#include "rdrs_rondb_connection_pool.hpp"
#include "status.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <drogon/HttpTypes.h>
#include <NdbMutex.h>
#include <NdbTick.h>
#include <ndb_init.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

/* Defined by main.cc in the server binary; tests provide their own. */
NdbMutex *globalConfigsMutex = nullptr;
extern RDRSRonDBConnectionPool *rdrsRonDBConnectionPool;

namespace {

/* Reserve a free TCP port for THIS process's probe listener: the config file
 * names the port of the mtr-started rdrs2 instance, which is already bound. */
uint16_t freePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
  uint16_t port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

/* Enough Ndb-object slots for the hammer threads below. */
constexpr Uint32 NUM_THREADS = 8;
constexpr Uint32 NUM_HAMMER_THREADS = 4;
constexpr Uint32 NUM_HEALTH_PROBES = 200;

/* Answer of one HTTP exchange against the probe port. */
struct ProbeAnswer {
  int status = -1;
  std::string body;
  Uint64 micros = 0;
};

ProbeAnswer probeGet(const char *path) {
  ProbeAnswer ans;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return ans;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(globalConfigs.rest.probePort);
  const NDB_TICKS start = NdbTick_getCurrentTicks();
  if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return ans;
  }
  std::string req = std::string("GET ") + path +
                    " HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
  if (::write(fd, req.data(), req.size()) !=
      static_cast<ssize_t>(req.size())) {
    ::close(fd);
    return ans;
  }
  std::string resp;
  char buf[1024];
  timeval tv{};
  tv.tv_sec = 5;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  ssize_t n;
  while ((n = ::read(fd, buf, sizeof(buf))) > 0) {
    resp.append(buf, static_cast<size_t>(n));
  }
  ::close(fd);
  ans.micros = NdbTick_Elapsed(start, NdbTick_getCurrentTicks()).microSec();
  if (resp.rfind("HTTP/1.1 ", 0) == 0 && resp.size() > 12) {
    ans.status = atoi(resp.c_str() + 9);
  }
  size_t sep = resp.find("\r\n\r\n");
  if (sep != std::string::npos) {
    ans.body = resp.substr(sep + 4);
  }
  return ans;
}

/* One server for the whole binary: the startup-contract test drives the
 * pool/drogon state around it in a fixed order, so the tests are ordered by
 * name (gtest runs them in declaration order within a suite). */
class ProbeServerClusterTest : public ::testing::Test {
 protected:
  static ProbeServer *server;

  static void SetUpTestSuite() {
    globalConfigs.rest.serverIP = "127.0.0.1";
    globalConfigs.rest.probeEnable = true;
    globalConfigs.rest.probePort = freePort();
    ASSERT_NE(globalConfigs.rest.probePort, 0);
    globalConfigs.security.tls.enableTLS = false;
    server = new ProbeServer();
    ASSERT_TRUE(server->Start());
  }

  static void TearDownTestSuite() {
    server->Stop();
    delete server;
    server = nullptr;
  }
};

ProbeServer *ProbeServerClusterTest::server = nullptr;

/* Phase 1: no pool yet, main server not up. BOTH endpoints answer 503 -
 * this is the window where main() has started the probe listener but the
 * (up to a minute long) RonDB connect has not finished, and neither the
 * startup probe (ping) nor readiness (health) may pass yet. The listener
 * answering AT ALL is the point: "starting" is a fast 503, "dead" is a
 * connection refusal. */
TEST_F(ProbeServerClusterTest, Phase1_NoPool_BothEndpoints503) {
  ProbeAnswer ping = probeGet(PING_PATH);
  EXPECT_EQ(ping.status, 503);
  ProbeAnswer health = probeGet(HEALTH_PATH);
  EXPECT_EQ(health.status, 503);
  EXPECT_EQ(health.body, "0");
}

/* Phase 2: pool up and connected, Drogon still down. A pod must NOT be
 * Ready yet - the main port refuses connections. */
TEST_F(ProbeServerClusterTest, Phase2_PoolUpDrogonDown_HealthStays503) {
  RS_Status status = RonDBConnection::init_rondb_connection(
    globalConfigs.ronDB, globalConfigs.ronDBMetadataCluster, NUM_THREADS);
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << status.message;
  ASSERT_GT(get_num_ready_data_nodes(), 0);

  ProbeAnswer health = probeGet(HEALTH_PATH);
  EXPECT_EQ(health.status, 503) << "Ready before the main port accepts";
  EXPECT_EQ(health.body, "0");
}

/* Phase 3: both conditions hold: ping 200 (startup probe passes) and
 * health 200 "1" (readiness passes). */
TEST_F(ProbeServerClusterTest, Phase3_DrogonUp_PingAndHealth200) {
  g_drogon_up.store(true, std::memory_order_release);
  ProbeAnswer ping = probeGet(PING_PATH);
  EXPECT_EQ(ping.status, 200);
  ProbeAnswer health = probeGet(HEALTH_PATH);
  EXPECT_EQ(health.status, 200);
  EXPECT_EQ(health.body, "1");
}

/* Phase 4: the never-blocks contract under the worst legal contention.
 * Hammer threads acquire and return METADATA Ndb objects as fast as
 * possible: unlike the thread-cached data path, every metadata acquisition
 * takes connectionMutex, and the object-creation branch holds BOTH
 * connection mutexes across Ndb creation - the exact critical section that
 * used to make /health block (same hammer as reconnect_test's
 * ContendedConnectionMutexStaysHealthy, but measured end to end over HTTP).
 * Every probe must answer 200 "1" (a wrong answer under load is the
 * /health-flapping bug), and fast: the bound is the ~20x1ms try-lock, so
 * 100ms of budget is generous and still well below the kubelet's 2s probe
 * timeout. */
TEST_F(ProbeServerClusterTest, Phase4_HealthFastAndCorrectUnderContention) {
  std::atomic<bool> stop{false};
  std::atomic<Uint64> acquisitions{0};
  std::vector<std::thread> hammers;
  hammers.reserve(NUM_HAMMER_THREADS);
  for (Uint32 i = 0; i < NUM_HAMMER_THREADS; i++) {
    hammers.emplace_back([&stop, &acquisitions]() {
      while (!stop.load(std::memory_order_acquire)) {
        Ndb *ndb = nullptr;
        if (rdrsRonDBConnectionPool->GetMetadataNdbObject(&ndb).http_code ==
              SUCCESS) {
          RS_Status ok = RS_OK;
          rdrsRonDBConnectionPool->ReturnMetadataNdbObject(ndb, &ok);
          acquisitions.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  Uint64 worst = 0;
  for (Uint32 i = 0; i < NUM_HEALTH_PROBES; i++) {
    ProbeAnswer health = probeGet(HEALTH_PATH);
    EXPECT_EQ(health.status, 200)
      << "healthy-but-busy server answered unhealthy (probe " << i << ")";
    EXPECT_EQ(health.body, "1");
    if (health.micros > worst) {
      worst = health.micros;
    }
  }
  stop.store(true, std::memory_order_release);
  for (auto &th : hammers) {
    th.join();
  }
  EXPECT_GT(acquisitions.load(), Uint64{0})
    << "The contending threads never acquired a metadata Ndb object, so "
       "connectionMutex was never contended and this test proved nothing";
  EXPECT_LT(worst, 100 * 1000)
    << "worst /health under contention took " << worst << "us";
  std::cout << "worst /health latency under contention: " << worst << "us\n";
}

/* Phase 5: teardown order. Drogon "stops" first (flag cleared), health goes
 * back to 503 while the pool still exists - the pod stops being Ready
 * before anything is torn down. */
TEST_F(ProbeServerClusterTest, Phase5_DrogonDown_HealthBackTo503) {
  g_drogon_up.store(false, std::memory_order_release);
  ProbeAnswer health = probeGet(HEALTH_PATH);
  EXPECT_EQ(health.status, 503);

  RS_Status status = RonDBConnection::shutdown_rondb_connection();
  ASSERT_EQ(status.http_code,
            static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    << status.message;

  /* Pool gone (nulled global): still a clean 503 on both, never a crash. */
  ProbeAnswer after = probeGet(HEALTH_PATH);
  EXPECT_EQ(after.status, 503);
  ProbeAnswer ping = probeGet(PING_PATH);
  EXPECT_EQ(ping.status, 503);
}

}  // namespace

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();

  std::string configFile;
  const char *env_config_file_path = std::getenv("RDRS_CONFIG_FILE");
  if (env_config_file_path != nullptr) {
    configFile = env_config_file_path;
  }
  RS_Status status = AllConfigs::init(configFile);
  if (status.http_code !=
        static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    std::cerr << "Error loading config: " << status.message << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  int rc = RUN_ALL_TESTS();
  NdbMutex_Destroy(globalConfigsMutex);
  ndb_end(0);
  return rc;
}
