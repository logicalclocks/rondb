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
 * HTTP conformance tests for the dedicated probe listener (probe_server.cpp).
 * Cluster-free: without a connection pool /ping answers 200 and /health
 * answers 503, which is exactly the startup-window contract. Every case here
 * is a probe-killing failure mode if the hand-rolled HTTP handling gets it
 * wrong (see the requirement list in probe_server.cpp).
 */

#include "probe_server.hpp"
#include "config_structs.hpp"
#include "constants.hpp"
#include "json_parser.hpp"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>

#include <NdbMutex.h>
#include <ndb_init.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

/* Defined by main.cc in the server binary; tests provide their own. */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

uint16_t g_probe_port = 0;

/* Reserve a free TCP port: bind port 0, read it back, close. The tiny race
 * until the server binds it is acceptable for a test. */
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

int dialProbe(uint16_t port) {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);
  if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return -1;
  }
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return fd;
}

/* IPv6 variants, for the ServerIP="::1" listener test. freePort6 doubles as
 * the IPv6-loopback availability check (0 = skip the test). */
uint16_t freePort6() {
  int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }
  sockaddr_in6 addr{};
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = in6addr_loopback;
  addr.sin6_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return 0;
  }
  socklen_t len = sizeof(addr);
  getsockname(fd, reinterpret_cast<sockaddr *>(&addr), &len);
  uint16_t port = ntohs(addr.sin6_port);
  ::close(fd);
  return port;
}

int dialProbe6(uint16_t port) {
  int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }
  sockaddr_in6 addr{};
  addr.sin6_family = AF_INET6;
  addr.sin6_addr = in6addr_loopback;
  addr.sin6_port = htons(port);
  if (::connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
    ::close(fd);
    return -1;
  }
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
  return fd;
}

void writeAll(int fd, const std::string &data) {
  size_t off = 0;
  while (off < data.size()) {
    ssize_t n = ::write(fd, data.data() + off, data.size() - off);
    ASSERT_GT(n, 0) << "write failed: " << strerror(errno);
    off += static_cast<size_t>(n);
  }
}

/* Read until EOF or timeout; returns whatever arrived. */
std::string readAvailable(int fd, int timeout_ms) {
  std::string out;
  char buf[4096];
  timeval tv{};
  tv.tv_sec = timeout_ms / 1000;
  tv.tv_usec = (timeout_ms % 1000) * 1000;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  while (true) {
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
      out.append(buf, static_cast<size_t>(n));
      continue;
    }
    break;  // EOF, timeout or error: the caller asserts on content
  }
  return out;
}

/* One-shot exchange on a fresh connection; reads to EOF/timeout.
 * NOT named "exchange": std::string arguments make ADL consider namespace
 * std, and for a two-argument call std::exchange(req, 3000) is the BETTER
 * overload (its T& binds a non-const lvalue better than const std::string&)
 * - it assigns 3000 to the request and returns the request's old value as
 * the "response". Found the hard way. */
std::string httpExchange(const std::string &request, int timeout_ms = 3000) {
  int fd = dialProbe(g_probe_port);
  EXPECT_GE(fd, 0) << "cannot connect to probe port";
  if (fd < 0) {
    return "";
  }
  writeAll(fd, request);
  std::string resp = readAvailable(fd, timeout_ms);
  ::close(fd);
  return resp;
}

std::string statusLine(const std::string &resp) {
  size_t eol = resp.find("\r\n");
  return eol == std::string::npos ? resp : resp.substr(0, eol);
}

std::string bodyOf(const std::string &resp) {
  size_t sep = resp.find("\r\n\r\n");
  return sep == std::string::npos ? "" : resp.substr(sep + 4);
}

bool hasHeader(const std::string &resp, const std::string &header) {
  return resp.find(header) != std::string::npos;
}

/* What kubelet's HTTP prober actually sends (Go net/http, keep-alives
 * disabled): this exact byte sequence must get a complete 200 answer. */
std::string kubeProbeRequest(const char *path) {
  std::string req = "GET ";
  req += path;
  req += " HTTP/1.1\r\n";
  req += "Host: 127.0.0.1:4407\r\n";
  req += "User-Agent: kube-probe/1.29\r\n";
  req += "Accept: */*\r\n";
  req += "Connection: close\r\n";
  req += "Accept-Encoding: gzip\r\n";
  req += "\r\n";
  return req;
}

class ProbeServerHttpTest : public ::testing::Test {
 protected:
  static ProbeServer *server;

  static void SetUpTestSuite() {
    g_probe_port = freePort();
    ASSERT_NE(g_probe_port, 0);
    globalConfigs.rest.serverIP = "127.0.0.1";
    globalConfigs.rest.probeEnable = true;
    globalConfigs.rest.probePort = g_probe_port;
    globalConfigs.security.tls.enableTLS = false;
    server = new ProbeServer();
    ASSERT_TRUE(server->Start());
    /* /ping is gated on the main server being up (so the startup probe can
     * use the probe port); these tests exercise the post-startup behaviour.
     * PingGatedUntilDrogonUp flips it off and back on. */
    g_drogon_up.store(true);
  }

  static void TearDownTestSuite() {
    server->Stop();
    delete server;
    server = nullptr;
  }
};

ProbeServer *ProbeServerHttpTest::server = nullptr;

TEST_F(ProbeServerHttpTest, KubeProbeReplayPing) {
  std::string resp = httpExchange(kubeProbeRequest(PING_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
  EXPECT_TRUE(hasHeader(resp, "content-length: 0")) << resp;
  EXPECT_TRUE(hasHeader(resp, "connection: close")) << resp;
  EXPECT_EQ(bodyOf(resp), "");
}

/* Cluster-free /health is the startup window: pool not initialised yet must
 * read as 503 "0", never a crash or a hang. */
TEST_F(ProbeServerHttpTest, KubeProbeReplayHealthNoCluster503) {
  std::string resp = httpExchange(kubeProbeRequest(HEALTH_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 503 Service Unavailable");
  EXPECT_TRUE(hasHeader(resp, "content-length: 1")) << resp;
  EXPECT_EQ(bodyOf(resp), "0");
}

/* g_drogon_up alone must not flip /health to 200: the pool is still absent,
 * and both conditions are required. */
TEST_F(ProbeServerHttpTest, DrogonUpAloneIsNotHealthy) {
  std::string resp = httpExchange(kubeProbeRequest(HEALTH_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 503 Service Unavailable");
}

/* The startup contract of /ping: 503 while the main server is not up (the
 * startup probe waits on this), 200 once it is. The flag is latched at
 * startup, so a data-path stall can never flip a 200 back - which is what
 * keeps the gate safe for liveness. */
TEST_F(ProbeServerHttpTest, PingGatedUntilDrogonUp) {
  g_drogon_up.store(false);
  std::string before = httpExchange(kubeProbeRequest(PING_PATH));
  g_drogon_up.store(true);
  std::string after = httpExchange(kubeProbeRequest(PING_PATH));
  EXPECT_EQ(statusLine(before), "HTTP/1.1 503 Service Unavailable");
  EXPECT_EQ(statusLine(after), "HTTP/1.1 200 OK");
}

/* HEAD = GET without body bytes; headers (incl. Content-Length) identical. */
TEST_F(ProbeServerHttpTest, HeadMatchesGetWithoutBody) {
  std::string get = httpExchange(kubeProbeRequest(HEALTH_PATH));
  std::string req = kubeProbeRequest(HEALTH_PATH);
  req.replace(0, 3, "HEAD");
  std::string head = httpExchange(req);
  EXPECT_EQ(statusLine(head), statusLine(get));
  EXPECT_TRUE(hasHeader(head, "content-length: 1")) << head;
  EXPECT_EQ(bodyOf(head), "");
}

TEST_F(ProbeServerHttpTest, PipelinedKeepAlive) {
  int fd = dialProbe(g_probe_port);
  ASSERT_GE(fd, 0);
  std::string one = "GET " + std::string(PING_PATH) + " HTTP/1.1\r\n"
                    "Host: x\r\n\r\n";
  /* Two requests in one write; the parser must serve both without waiting
   * for more input. Second one closes. */
  writeAll(fd, one + kubeProbeRequest(PING_PATH));
  std::string resp = readAvailable(fd, 3000);
  ::close(fd);
  size_t first = resp.find("HTTP/1.1 200 OK");
  ASSERT_NE(first, std::string::npos) << resp;
  size_t second = resp.find("HTTP/1.1 200 OK", first + 1);
  EXPECT_NE(second, std::string::npos)
    << "second pipelined response missing:\n" << resp;
  EXPECT_TRUE(hasHeader(resp.substr(0, second), "connection: keep-alive"));
}

/* One read callback serves at most kMaxPipelinedRequests before closing
 * the connection: kubelet never pipelines, so only an abusive client
 * blasting requests without reading responses hits this - unbounded, one
 * connection could monopolize the single loop thread and starve the very
 * probes the port exists for. The burst is large enough that any plausible
 * split into read callbacks still carries one over-limit callback. */
TEST_F(ProbeServerHttpTest, PipelineFloodIsCutOff) {
  int fd = dialProbe(g_probe_port);
  ASSERT_GE(fd, 0);
  std::string one = "GET " + std::string(PING_PATH) + " HTTP/1.1\r\n"
                    "Host: x\r\n\r\n";
  constexpr size_t kBurst = 1000;
  std::string burst;
  burst.reserve(one.size() * kBurst);
  for (size_t i = 0; i < kBurst; i++) {
    burst += one;
  }
  writeAll(fd, burst);
  std::string resp = readAvailable(fd, 5000);
  ::close(fd);
  size_t served = 0;
  for (size_t pos = resp.find("HTTP/1.1 200 OK"); pos != std::string::npos;
       pos = resp.find("HTTP/1.1 200 OK", pos + 1)) {
    served++;
  }
  EXPECT_GE(served, 1U) << resp.substr(0, 200);
  EXPECT_LT(served, kBurst) << "pipeline flood was never cut off";
  /* Other clients must be served while and after the flood. */
  EXPECT_EQ(statusLine(httpExchange(kubeProbeRequest(PING_PATH))),
            "HTTP/1.1 200 OK");
}

/* A request arriving one byte at a time (many read callbacks per request)
 * must still be answered. */
TEST_F(ProbeServerHttpTest, ByteAtATime) {
  int fd = dialProbe(g_probe_port);
  ASSERT_GE(fd, 0);
  std::string req = kubeProbeRequest(PING_PATH);
  for (char c : req) {
    writeAll(fd, std::string(1, c));
  }
  std::string resp = readAvailable(fd, 3000);
  ::close(fd);
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
}

/* Bare-LF line endings must not wedge the parser. */
TEST_F(ProbeServerHttpTest, BareLineFeeds) {
  std::string resp =
    httpExchange("GET " + std::string(PING_PATH) + " HTTP/1.1\nHost: x\n\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
}

TEST_F(ProbeServerHttpTest, QueryStringIgnored) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + "?verbose=1 HTTP/1.1\r\nHost: x\r\n"
    "Connection: close\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
}

TEST_F(ProbeServerHttpTest, TrailingSlashIs404) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + "/ HTTP/1.1\r\nHost: x\r\n"
    "Connection: close\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 404 Not Found");
}

TEST_F(ProbeServerHttpTest, UnknownPathIs404) {
  std::string resp = httpExchange(
    "GET /0.1.0/batch HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 404 Not Found");
}

TEST_F(ProbeServerHttpTest, WrongMethodIs405) {
  std::string resp = httpExchange(
    "POST " + std::string(PING_PATH) + " HTTP/1.1\r\nHost: x\r\n"
    "Connection: close\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 405 Method Not Allowed");
}

TEST_F(ProbeServerHttpTest, Http10DefaultsToClose) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + " HTTP/1.0\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
  EXPECT_TRUE(hasHeader(resp, "connection: close")) << resp;
}

/* A GET with a body is answered 400 and the connection closed - skipping an
 * unread body would risk protocol desync. */
TEST_F(ProbeServerHttpTest, GetWithBodyIs400AndClose) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + " HTTP/1.1\r\nHost: x\r\n"
    "Content-Length: 3\r\n\r\nabc");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 400 Bad Request");
  EXPECT_TRUE(hasHeader(resp, "connection: close")) << resp;
}

/* Repeated Content-Length headers are rejected outright: a later value
 * overriding an earlier one is the classic request-smuggling ambiguity,
 * and a "0" repeat would otherwise mask real body bytes that then desync
 * the connection as the next request. */
TEST_F(ProbeServerHttpTest, DuplicateContentLengthIs400AndClose) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + " HTTP/1.1\r\nHost: x\r\n"
    "Content-Length: 3\r\nContent-Length: 0\r\n\r\nabc");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 400 Bad Request");
  EXPECT_TRUE(hasHeader(resp, "connection: close")) << resp;
  /* Same, values agreeing - still ambiguous per RFC 7230 3.3.2. */
  resp = httpExchange(
    "GET " + std::string(PING_PATH) + " HTTP/1.1\r\nHost: x\r\n"
    "Content-Length: 0\r\nContent-Length: 0\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 400 Bad Request");
  EXPECT_TRUE(hasHeader(resp, "connection: close")) << resp;
}

TEST_F(ProbeServerHttpTest, ChunkedIs400AndClose) {
  std::string resp = httpExchange(
    "GET " + std::string(PING_PATH) + " HTTP/1.1\r\nHost: x\r\n"
    "Transfer-Encoding: chunked\r\n\r\n");
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 400 Bad Request");
}

TEST_F(ProbeServerHttpTest, OversizedHeadersAre431AndClose) {
  std::string req = "GET " + std::string(PING_PATH) + " HTTP/1.1\r\n";
  req += "X-Filler: " + std::string(10 * 1024, 'a') + "\r\n";
  /* No terminating blank line: the size cap must trip on the incomplete
   * block, not wait for a terminator that may never come. */
  std::string resp = httpExchange(req, 3000);
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 431 Request Header Fields Too Large");
}

TEST_F(ProbeServerHttpTest, OversizedCompleteHeadersAre431AndClose) {
  std::string req = "GET " + std::string(PING_PATH) + " HTTP/1.1\r\n";
  req += "X-Filler: " + std::string(10 * 1024, 'a') + "\r\n";
  req += "\r\n";
  /* COMPLETE header block over the cap: must be rejected before parsing.
   * Checking only the incomplete path lets a client that delivers its
   * oversized headers in one read bypass the limit and get a 200. */
  std::string resp = httpExchange(req, 3000);
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 431 Request Header Fields Too Large");
  EXPECT_EQ(resp.find("HTTP/1.1 200"), std::string::npos);
}

/* Slowloris: a connection dribbling an incomplete request is force-closed by
 * the sweep timer (trantor's idle kickoff resets on every read and cannot
 * catch this), and service to other clients continues meanwhile. */
TEST_F(ProbeServerHttpTest, SlowlorisIsSweptWhileOthersAreServed) {
  int slow = dialProbe(g_probe_port);
  ASSERT_GE(slow, 0);
  writeAll(slow, "GET /0.");  // incomplete forever

  /* While the dribbler sits there, normal probes answer. */
  std::string resp = httpExchange(kubeProbeRequest(PING_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");

  /* The deadline is 2s + sweep 0.5s; well within 5s the socket must be
   * closed (read returns EOF/reset, not timeout). */
  timeval tv{};
  tv.tv_sec = 5;
  setsockopt(slow, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
  char buf[64];
  ssize_t n = ::read(slow, buf, sizeof(buf));
  if (n < 0) {
    EXPECT_NE(errno, EAGAIN) << "slowloris connection was not closed in time";
    EXPECT_NE(errno, EWOULDBLOCK);
  } else {
    EXPECT_EQ(n, 0) << "unexpected data on the slowloris connection";
  }
  ::close(slow);
}

/* Idle connections beyond the cap are closed on accept; service continues
 * for the ones within it and for later clients. */
TEST_F(ProbeServerHttpTest, ConnectionCapEnforced) {
  constexpr int kOverCap = 140;  // cap is 128
  std::vector<int> held;
  held.reserve(kOverCap);
  for (int i = 0; i < kOverCap; i++) {
    int fd = dialProbe(g_probe_port);
    ASSERT_GE(fd, 0);
    held.push_back(fd);
  }
  /* Give the single loop a moment to accept + force-close the excess. */
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  int closed = 0;
  for (int fd : held) {
    char buf[8];
    timeval tv{};
    tv.tv_usec = 50 * 1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK)) {
      closed++;
    }
  }
  EXPECT_GE(closed, kOverCap - 128) << "over-cap connections were not closed";

  for (int fd : held) {
    ::close(fd);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  std::string resp = httpExchange(kubeProbeRequest(PING_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
}

/* A taken port must fail Start() cleanly - never trantor's exit(1). */
TEST_F(ProbeServerHttpTest, BindConflictFailsCleanly) {
  ProbeServer second;
  /* Same port as the running server. */
  EXPECT_FALSE(second.Start());
  second.Stop();  // idempotent on a never-started server
  /* And the original keeps serving. */
  std::string resp = httpExchange(kubeProbeRequest(PING_PATH));
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");
}

}  // namespace

/*
 * TLS: the listener mirrors the main port's certificate and must not accept
 * plaintext when TLS is on. Runs on its own port/server since the suite
 * server is plaintext. The full TLS handshake (and the never-require-client-
 * cert rule) is exercised end to end by the Go integration tests against the
 * MTR TLS instance; here the guard is that plaintext cannot get an answer.
 */
namespace {

const char kTestCertPem[] =
  "-----BEGIN CERTIFICATE-----\n"
  "MIIDFTCCAf2gAwIBAgIUF8iWQK+gmuzbBltkNp4AP6+cSiUwDQYJKoZIhvcNAQEL\n"
  "BQAwGjEYMBYGA1UEAwwPcmRycy1wcm9iZS10ZXN0MB4XDTI2MDkyNTEzNDIxOFoX\n"
  "DTQ2MDkyMDEzNDIxOFowGjEYMBYGA1UEAwwPcmRycy1wcm9iZS10ZXN0MIIBIjAN\n"
  "BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA1dkeyQxvUCsowb1YV3L7NaAyQ55M\n"
  "KVtrR3hyKyHNQrNXB0D40XvVycu1X6idjqPXiOQqfai0IdaL7RPMy3shoYBjlv8i\n"
  "htT6zU14Peyef2xumP3Lk3l7rZAr4uqNLMidSr9bIDsQ6fK/zHQwFqLzz5iw9pRy\n"
  "PQG6YFpXu/1hSBEFls6SdFdQkQJN4qYiOPSBudoPGsQtEIMVOxwQNMWQtoQ/wTQV\n"
  "lZaYHS7/+YXOvb9NKnL9SPIVP2d4JjuFR7d86YUYvaGB6ZGfosXM7CgPhHMN0Jfu\n"
  "k+4fkDy6sVOKBVRFgAl/6oXOQmwOv2+QK/AUCNHzwI9hg/8iqGaQkQxxhwIDAQAB\n"
  "o1MwUTAdBgNVHQ4EFgQUFKiUUbxOSo8SziVA/iBZrHAuWIUwHwYDVR0jBBgwFoAU\n"
  "FKiUUbxOSo8SziVA/iBZrHAuWIUwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B\n"
  "AQsFAAOCAQEANq6e/jRiluxSFH9+7hOoCjAcVGCnLUEYE8sVqU1Xtd/QhrTvsaqb\n"
  "eaD1qKQeBwPdM8/FlMUkqn02q+y4+rzFynWZBPaci8mc0SCOaHXgCMChC7je3IGu\n"
  "0DMEC4mQl5nCdwDB5fhR+8FtktW0i7sTksTg1ZsBLcwksdwYVtpYgwcaDXmQdBGh\n"
  "5NjcTdVs+LYh0uQJd88d50xinuwI8t6kfrm9XohY6f7VT2CVHG15qf8dgZSuq4nT\n"
  "3L7JpE6QT93xQ2B+KCe93Fg2Gc2GLYwgnRowfG3weAXDIexsswp5ydOXn5WyuFgF\n"
  "gVYatfDUfOcjY0+MLvz8LI5HOAxNCsldaA==\n"
  "-----END CERTIFICATE-----\n";

const char kTestKeyPem[] =
  "-----BEGIN PRIVATE KEY-----\n"
  "MIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQDV2R7JDG9QKyjB\n"
  "vVhXcvs1oDJDnkwpW2tHeHIrIc1Cs1cHQPjRe9XJy7VfqJ2Oo9eI5Cp9qLQh1ovt\n"
  "E8zLeyGhgGOW/yKG1PrNTXg97J5/bG6Y/cuTeXutkCvi6o0syJ1Kv1sgOxDp8r/M\n"
  "dDAWovPPmLD2lHI9AbpgWle7/WFIEQWWzpJ0V1CRAk3ipiI49IG52g8axC0QgxU7\n"
  "HBA0xZC2hD/BNBWVlpgdLv/5hc69v00qcv1I8hU/Z3gmO4VHt3zphRi9oYHpkZ+i\n"
  "xczsKA+Ecw3Ql+6T7h+QPLqxU4oFVEWACX/qhc5CbA6/b5Ar8BQI0fPAj2GD/yKo\n"
  "ZpCRDHGHAgMBAAECgf8rKKa4l3SKZCuAQ7aQmk7Dg+ahFFGgByifmVocBQu9anR7\n"
  "V6GKpcjVRpz/BrNwa6C+//g+Ds5MBgDiLXInnwd/5hQzZUqSlmldBfA+jy1t33Ry\n"
  "wXCp/YVN13Wuq6fSYgAHa0H46fLVRH4bxVdEj56lRyvFQtsgyjh95Gh7Mv1vOExt\n"
  "9HzAO81k5EyMzy3g94M/f6i1nipypZT3ZAjUcRNUaQhDK9CfWFlMpUhH8p2BoqSe\n"
  "NxyTJpXQqqjfXSJ6786diKtlynt4MWOk8MxnNJpcfn8M8AVV0jjzV0Gxb8nFq2vd\n"
  "yYvbx8sZ2qoj8JY47MpVZejQMyMqKNr7pS3tfKECgYEA8Gs0qlDjvimJMGowMvid\n"
  "WwO/OkkQ/gi5DHOoUj+zmG7E00vk1FbisCBrWm7TBaRZtyo/I7w7euF6lgUTNCZI\n"
  "ucYQUCR0LOJlEzOG9b4DAZyZlIkBIcZdEdwgl9zM3xlUTwLwFoQi4vNndj0Y3Ws3\n"
  "y01lVMzJ+K/raNs+hlv+z3cCgYEA47UUic1MfschctEcb/iY7X3kyeDYIyTLid61\n"
  "zWFeabj0IhsdUsilLx3S7pN3112F4Lg7y7momOJ1RDTcTpmlflaeUF69ng9jwHDq\n"
  "EO9dLcf83Psd4OwUEiNHtAD82cizB6cB5V/P/7cqF7XY4DHMCldkz3QOUzQNBRGx\n"
  "+IEHknECgYBpW5HI0Yn8W9dzEBXvQGQ07n9u23ZG3Su6+TRaVvAtbN10e13cb/cH\n"
  "mC1zg/2WC2AFlM32qxal0woVlEPGJsDYKKQdetwuj0gcEgiiyJIosqfbH+8PDg7b\n"
  "NMxTwL9HRaJcvbzZIS7opiJA/qVW4xWgUlqFvvkDspRHb00HNGmGIQKBgQDVvWr6\n"
  "8u+D7U1TZkAoRpTeEJdKfDjFvEsmLhw/Hc+us4LN5N/AjkCnmnodoeUTDmGVj7np\n"
  "QGumnqNuk6PcT9MNZScTz+pzTITY5eSAYv7280tC7qCcOV2ZrO4oY+j0ULTkUPqx\n"
  "oR8wLHFhcjuSLowVhPVG2ex8Y1Z5VKPW3N8LsQKBgAJxiRMcrvKB0H6Fb2hxBpuw\n"
  "rEttrkYnpNfslNVYkhPRsj/EJyL34dPmLlclcTT4uZxLK/e9acDVfIFxxKZiNWYj\n"
  "vfZ4XosXf0vbhfv67ZqWWwY+AWXGnK8temk2xG9rOCrVnVt3cpzaKszvMZrr9tm6\n"
  "V8JGzhnLh+UV+BPICEUx\n"
  "-----END PRIVATE KEY-----\n";

std::string writeTempPem(const char *name, const char *pem) {
  std::string path = ::testing::TempDir() + name;
  FILE *f = fopen(path.c_str(), "w");
  EXPECT_NE(f, nullptr);
  if (f != nullptr) {
    fputs(pem, f);
    fclose(f);
  }
  return path;
}

/* ServerIP with an IPv6 literal must bind an IPv6 listener. The trantor
 * InetAddress(ip, port) constructor defaults to IPv4 and silently turns
 * "::1" into 0.0.0.0 - the listener then accepts on IPv4 and refuses the
 * configured address. */
TEST(ProbeServerIpv6Test, Ipv6LiteralBindsIpv6NotIpv4) {
  uint16_t port = freePort6();
  if (port == 0) {
    GTEST_SKIP() << "IPv6 loopback not available";
  }
  AllConfigs saved = globalConfigs;
  globalConfigs.rest.serverIP = "::1";
  globalConfigs.rest.probeEnable = true;
  globalConfigs.rest.probePort = port;
  globalConfigs.security.tls.enableTLS = false;

  ProbeServer server;
  ASSERT_TRUE(server.Start());
  g_drogon_up.store(true);

  int fd6 = dialProbe6(port);
  ASSERT_GE(fd6, 0) << "IPv6 connect refused: listener bound the wrong family";
  writeAll(fd6, kubeProbeRequest(PING_PATH));
  std::string resp = readAvailable(fd6, 3000);
  ::close(fd6);
  EXPECT_EQ(statusLine(resp), "HTTP/1.1 200 OK");

  /* The buggy default-IPv4 constructor yielded 0.0.0.0: IPv4 must NOT be
   * accepted when ServerIP is an IPv6 loopback literal. */
  int fd4 = dialProbe(port);
  EXPECT_LT(fd4, 0) << "IPv4 accepted despite ServerIP=\"::1\"";
  if (fd4 >= 0) {
    ::close(fd4);
  }

  server.Stop();
  globalConfigs = saved;
}

TEST(ProbeServerTlsTest, PlaintextGetsNoAnswerWhenTlsIsOn) {
  uint16_t port = freePort();
  ASSERT_NE(port, 0);
  AllConfigs saved = globalConfigs;
  globalConfigs.rest.serverIP = "127.0.0.1";
  globalConfigs.rest.probeEnable = true;
  globalConfigs.rest.probePort = port;
  globalConfigs.security.tls.enableTLS = true;
  globalConfigs.security.tls.certificateFile =
    writeTempPem("probe_test_cert.pem", kTestCertPem);
  globalConfigs.security.tls.privateKeyFile =
    writeTempPem("probe_test_key.pem", kTestKeyPem);

  ProbeServer server;
  ASSERT_TRUE(server.Start());

  int fd = dialProbe(port);
  ASSERT_GE(fd, 0);
  writeAll(fd, kubeProbeRequest(PING_PATH));
  std::string resp = readAvailable(fd, 2000);
  ::close(fd);
  EXPECT_EQ(resp.find("HTTP/1.1 200"), std::string::npos)
    << "TLS listener answered a plaintext request";

  server.Stop();
  globalConfigs = saved;
}

/* A bad certificate path must fail Start() cleanly, not throw or exit. */
TEST(ProbeServerTlsTest, BadCertFailsCleanly) {
  uint16_t port = freePort();
  ASSERT_NE(port, 0);
  AllConfigs saved = globalConfigs;
  globalConfigs.rest.serverIP = "127.0.0.1";
  globalConfigs.rest.probeEnable = true;
  globalConfigs.rest.probePort = port;
  globalConfigs.security.tls.enableTLS = true;
  globalConfigs.security.tls.certificateFile = "/nonexistent/cert.pem";
  globalConfigs.security.tls.privateKeyFile = "/nonexistent/key.pem";

  ProbeServer server;
  EXPECT_FALSE(server.Start());
  server.Stop();
  globalConfigs = saved;
}

/* Config validation through the real parser+validator: the PROBLEM rules. */
TEST(ProbeConfigTest, ProbePortCollisionsRejected) {
  {
    AllConfigs c = globalConfigs;
    c.rest.serverPort = 4406;
    c.rest.probeEnable = true;
    c.rest.probePort = 4406;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_NE(st.http_code, 200)
      << "probe port equal to the REST port must be rejected";
  }
  {
    AllConfigs c = globalConfigs;
    c.rest.probeEnable = true;
    c.rest.probePort = 0;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_NE(st.http_code, 200) << "probe port zero must be rejected";
  }
  {
    /* Rondis collision only matters when rondis is enabled; otherwise the
     * same value is harmless and must be accepted. */
    AllConfigs c = globalConfigs;
    c.rest.probeEnable = true;
    c.rest.probePort = c.rondis.serverPort;
    c.rondis.enable = false;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_EQ(st.http_code, 200)
      << "harmless rondis port overlap rejected: " << st.message;
  }
}

/* The probe port never authenticates, so requiring auth for ping/health
 * while the probe port is enabled is a config error, not a silent bypass:
 * API-key validation reads RonDB and can stall for tens of seconds, which a
 * probe endpoint must never do. With the flags inert (no API keys at all,
 * or ProbeEnable off) the same settings pass. */
TEST(ProbeConfigTest, ProbePortWithAuthenticatedProbesIsRejected) {
  AllConfigs base = globalConfigs;
  base.rest.probeEnable = true;
  base.security.apiKey.useHopsworksAPIKeys = true;
  base.security.insecureAllowAll = false;
  {
    AllConfigs c = base;
    c.rest.pingRequiresAuth = true;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_NE(st.http_code, 200);
    EXPECT_NE(std::string(st.message).find("ProbeEnable"), std::string::npos)
      << "message was: " << st.message;
  }
  {
    AllConfigs c = base;
    c.rest.healthRequiresAuth = true;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_NE(st.http_code, 200);
  }
  {
    /* Same flags with the probe port off: valid (authenticated ping/health
     * on the main port is the documented alternative). */
    AllConfigs c = base;
    c.rest.probeEnable = false;
    c.rest.pingRequiresAuth = true;
    c.rest.healthRequiresAuth = true;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_EQ(st.http_code, 200) << st.message;
  }
  {
    /* The common no-auth deployment (InsecureAllowAll) with the probe port
     * on: valid. (A stray RequiresAuth flag without API keys is already
     * unrepresentable: the InsecureAllowAll cross-checks reject it, so the
     * new rule cannot break any previously-valid config on upgrade.) */
    AllConfigs c = base;
    c.security.apiKey.useHopsworksAPIKeys = false;
    c.security.insecureAllowAll = true;
    c.rest.pingRequiresAuth = false;
    c.rest.healthRequiresAuth = false;
    RS_Status st = AllConfigs::set_all(c);
    EXPECT_EQ(st.http_code, 200) << st.message;
  }
}

/* Pins the exact failure signature of a version-skewed rollout: an rdrs2
 * image that predates REST.ProbePort rejects a config carrying it with
 * "Unexpected key" at startup (this binary's parser is equally strict about
 * keys IT does not know, which is what is asserted here). The rondb-helm
 * runbook matches on this text; if the wording changes, update both. */
TEST(ProbeConfigTest, UnknownRestKeyIsRejectedWithUnexpectedKey) {
  AllConfigs parsed;
  RS_Status st = JSONParser::config_parse(
    R"({"REST": {"FutureUnknownOption": 1}})", parsed);
  EXPECT_NE(st.http_code, 200);
  EXPECT_NE(std::string(st.message).find("Unexpected key"), std::string::npos)
    << "message was: " << st.message;
}

}  // namespace

int main(int argc, char **argv) {
  ndb_init();
  globalConfigsMutex = NdbMutex_Create();
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  ndb_end(0);
  return ret;
}
