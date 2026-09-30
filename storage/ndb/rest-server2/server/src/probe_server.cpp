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

#include "probe_server.hpp"
#include "config_structs.hpp"
#include "constants.hpp"
#include "logger.hpp"
#include "metrics.hpp"
#include <rdrs_dal.h>

#include <trantor/net/EventLoopThread.h>
#include <trantor/net/TcpServer.h>
#include <trantor/net/TcpConnection.h>
#include <trantor/net/InetAddress.h>
#include <trantor/net/TLSPolicy.h>
#include <trantor/utils/MsgBuffer.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <future>
#include <string>
#include <vector>
#include <util/require.h>

std::atomic<bool> g_drogon_up{false};

namespace {

/* A request whose header block exceeds this is rejected with 431 and the
 * connection closed: trantor's MsgBuffer grows without bound on reads, and
 * this is the only cap. Kubelet probes are a few hundred bytes. */
constexpr size_t kMaxHeaderBytes = 8 * 1024;
/* More concurrent connections than this are closed on accept. Probes open
 * one short-lived connection each; this only bounds abuse of the single
 * loop thread. */
constexpr size_t kMaxConnections = 128;
/* A request that stays incomplete this long after its first byte is
 * force-closed by the sweep timer (slowloris; trantor's idle kickoff resets
 * on every read so it cannot catch a dribbling client). Matches the probe
 * timeout: a client slower than this has already failed its probe. */
constexpr auto kRequestDeadline = std::chrono::seconds(2);
constexpr double kSweepIntervalS = 0.5;
/* Fully idle (no bytes at all) connections are closed by trantor's timing
 * wheel after this many seconds. */
constexpr size_t kIdleTimeoutS = 10;

struct ParsedRequest {
  std::string method;
  std::string target;  // path only, query string stripped
  bool http11 = true;
  bool keep_alive = true;
  bool has_body = false;      // Content-Length > 0
  bool has_chunked = false;   // any Transfer-Encoding
  size_t consumed = 0;        // bytes of the header block incl. terminator
};

/* Case-insensitive ASCII compare, needed for header names. */
bool iequals(const std::string &a, const char *b) {
  size_t blen = strlen(b);
  if (a.size() != blen) {
    return false;
  }
  for (size_t i = 0; i < blen; i++) {
    if (tolower(static_cast<unsigned char>(a[i])) !=
        tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

std::string trimmed(const std::string &s) {
  size_t b = s.find_first_not_of(" \t");
  if (b == std::string::npos) {
    return "";
  }
  size_t e = s.find_last_not_of(" \t");
  return s.substr(b, e - b + 1);
}

/* Find the end of the header block. Returns npos when incomplete. Accepts
 * bare-LF line endings (a lenient client must not be able to wedge the
 * parser - worst case it gets a 400, never a hang). */
size_t findHeaderEnd(const char *data, size_t len) {
  for (size_t i = 0; i + 1 < len; i++) {
    if (data[i] == '\n') {
      if (data[i + 1] == '\n') {
        return i + 2;
      }
      if (i + 2 < len && data[i + 1] == '\r' && data[i + 2] == '\n') {
        return i + 3;
      }
    }
  }
  return std::string::npos;
}

/* Parse one request's header block (request line + headers). Returns false
 * on anything malformed; the caller answers 400 and closes. */
bool parseRequest(const char *data, size_t headerEnd, ParsedRequest *req) {
  std::string block(data, headerEnd);
  req->consumed = headerEnd;

  size_t lineEnd = block.find('\n');
  if (lineEnd == std::string::npos) {
    return false;
  }
  std::string requestLine = block.substr(0, lineEnd);
  if (!requestLine.empty() && requestLine.back() == '\r') {
    requestLine.pop_back();
  }
  size_t sp1 = requestLine.find(' ');
  size_t sp2 = requestLine.rfind(' ');
  if (sp1 == std::string::npos || sp2 == sp1) {
    return false;
  }
  req->method = requestLine.substr(0, sp1);
  std::string target = requestLine.substr(sp1 + 1, sp2 - sp1 - 1);
  std::string version = requestLine.substr(sp2 + 1);
  if (version == "HTTP/1.1") {
    req->http11 = true;
  } else if (version == "HTTP/1.0") {
    req->http11 = false;
  } else {
    return false;
  }
  size_t query = target.find('?');
  req->target =
    (query == std::string::npos) ? target : target.substr(0, query);
  if (req->target.empty() || req->method.empty()) {
    return false;
  }

  /* HTTP/1.1 defaults to keep-alive, HTTP/1.0 to close. */
  req->keep_alive = req->http11;

  size_t pos = lineEnd + 1;
  bool seen_content_length = false;
  while (pos < headerEnd) {
    size_t next = block.find('\n', pos);
    if (next == std::string::npos) {
      break;
    }
    std::string line = block.substr(pos, next - pos);
    pos = next + 1;
    if (!line.empty() && line.back() == '\r') {
      line.pop_back();
    }
    if (line.empty()) {
      break;  // end of headers
    }
    size_t colon = line.find(':');
    if (colon == std::string::npos) {
      return false;
    }
    std::string name = trimmed(line.substr(0, colon));
    std::string value = trimmed(line.substr(colon + 1));
    if (iequals(name, "content-length")) {
      /* Repeated Content-Length headers make the body length ambiguous
       * (request smuggling / connection desync, RFC 7230 3.3.2): a later
       * value would silently override an earlier one and leave unread
       * body bytes to be parsed as the NEXT request. Reject the request
       * even when the repeated values agree. */
      if (seen_content_length) {
        return false;
      }
      seen_content_length = true;
      char *end = nullptr;
      unsigned long long cl = strtoull(value.c_str(), &end, 10);
      if (end == value.c_str() || *end != '\0') {
        return false;
      }
      req->has_body = cl > 0;
    } else if (iequals(name, "transfer-encoding")) {
      req->has_chunked = true;
    } else if (iequals(name, "connection")) {
      if (iequals(value, "close")) {
        req->keep_alive = false;
      } else if (iequals(value, "keep-alive")) {
        req->keep_alive = true;
      }
    }
  }
  return true;
}

const char *reasonPhrase(int status) {
  switch (status) {
    case 200: return "OK";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 431: return "Request Header Fields Too Large";
    case 503: return "Service Unavailable";
    default:  return "Internal Server Error";
  }
}

/* Content-Length is always emitted, even 0: without it an HTTP/1.1 client
 * waits for EOF and a keep-alive probe times out. HEAD gets the same
 * headers as GET but no body bytes (RFC 9110). Content-Type matches
 * Drogon's default for newHttpResponse() so the two ports answer alike. */
std::string buildResponse(int status,
                          const std::string &body,
                          bool keep_alive,
                          bool head) {
  std::string resp;
  resp.reserve(160 + body.size());
  resp += "HTTP/1.1 ";
  resp += std::to_string(status);
  resp += " ";
  resp += reasonPhrase(status);
  resp += "\r\ncontent-length: ";
  resp += std::to_string(body.size());
  resp += "\r\ncontent-type: text/html; charset=utf-8";
  resp += keep_alive ? "\r\nconnection: keep-alive"
                     : "\r\nconnection: close";
  resp += "\r\n\r\n";
  if (!head) {
    resp += body;
  }
  return resp;
}

/* The health verdict, from state only - never blocks (see the plan in
 * health_ctrl.cpp for why one STARTED data node implies a serving cluster;
 * this is the same predicate, plus the Drogon guard):
 * - g_drogon_up: readiness must not say 200 before the main port accepts.
 * - get_rondb_stats()/get_num_ready_data_nodes() are wait-free and safe
 *   before init() and after shutdown (they answer "down" then). */
int healthStatus() {
  if (!g_drogon_up.load(std::memory_order_acquire)) {
    return 503;
  }
  RonDB_Stats stats;
  (void)get_rondb_stats(&stats);
  if (stats.connection_state == CONNECTED &&
      !stats.is_reconnection_in_progress &&
      get_num_ready_data_nodes() > 0) {
    return 200;
  }
  return 503;
}

/* Pre-check that the port can be bound: trantor's Socket::bindAddress
 * calls exit(1) on failure, which would bypass do_exit() teardown. Binding
 * a throwaway socket first turns that into a clean, loggable failure. */
bool canBind(const std::string &ip, uint16_t port, std::string *err) {
  /* The ipv6 flag must be passed explicitly: InetAddress(ip, port) defaults
   * to IPv4 and silently turns an IPv6 literal like "::1" into 0.0.0.0, so
   * this check would "validate" an address the listener never binds. An
   * IPv6 literal always contains ':'. */
  trantor::InetAddress addr(ip, port, ip.find(':') != std::string::npos);
  int fd = ::socket(addr.isIpV6() ? AF_INET6 : AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    *err = strerror(errno);
    return false;
  }
  int one = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  const socklen_t addrlen = addr.isIpV6()
    ? static_cast<socklen_t>(sizeof(struct sockaddr_in6))
    : static_cast<socklen_t>(sizeof(struct sockaddr_in));
  int ret = ::bind(fd, addr.getSockAddr(), addrlen);
  if (ret != 0) {
    *err = strerror(errno);
    ::close(fd);
    return false;
  }
  ::close(fd);
  return true;
}

}  // namespace

ProbeServer::ProbeServer() = default;

ProbeServer::~ProbeServer() {
  Stop();
}

bool ProbeServer::Start() {
  require(!m_started);
  const std::string &ip = globalConfigs.rest.serverIP;
  const uint16_t port = globalConfigs.rest.probePort;

  std::string err;
  if (!canBind(ip, port, &err)) {
    /* Also on stderr: this runs during startup, where the other fatal
     * config errors print to stderr, and it must be visible even if the
     * event logger's handlers are not fully set up yet. */
    std::string msg =
      "Probe listener cannot bind " + ip + ":" + std::to_string(port) +
      " (" + err + "). Change REST.ProbePort, or set REST.ProbeEnable to"
      " false to disable the probe listener.";
    fprintf(stderr, "%s\n", msg.c_str());
    rdrs_logger::error(msg);
    return false;
  }

  m_loop_thread =
    std::make_unique<trantor::EventLoopThread>("rdrs-probe");
  m_loop_thread->run();
  trantor::EventLoop *loop = m_loop_thread->getLoop();

  /* reUsePort MUST stay false: trantor defaults it to true, and with
   * SO_REUSEPORT a second rdrs2 process binds the same port silently and
   * the kernel splits the probes between the two. The ipv6 flag must be
   * passed explicitly, as in canBind(): the default-IPv4 constructor turns
   * an IPv6 ServerIP into 0.0.0.0 and the listener binds the wrong
   * family. */
  m_server = std::make_unique<trantor::TcpServer>(
    loop,
    trantor::InetAddress(ip, port, ip.find(':') != std::string::npos),
    "rdrs-probe",
    /*reUseAddr*/ true, /*reUsePort*/ false);

  if (globalConfigs.security.tls.enableTLS) {
    /* Mirrors the main listener's certificate, but never demands a client
     * certificate even when RequireAndVerifyClientCert is set: kubelet
     * HTTPS probes present none, and enforcing it here would fail every
     * probe in the fleet. defaultServerPolicy() sets validate(false). */
    try {
      m_server->enableSSL(trantor::TLSPolicy::defaultServerPolicy(
        globalConfigs.security.tls.certificateFile,
        globalConfigs.security.tls.privateKeyFile));
    } catch (const std::exception &e) {
      rdrs_logger::error(
        std::string("Probe listener TLS setup failed: ") + e.what());
      /* stop() tears the acceptor down ON the loop thread (destroying it
       * from this thread trips trantor's loop-affinity check), then the
       * thread can be joined and everything destroyed. */
      m_server->stop();
      m_server.reset();
      m_loop_thread.reset();
      return false;
    }
  }

  m_server->setConnectionCallback(
    [this](const trantor::TcpConnectionPtr &conn) { onConnection(conn); });
  m_server->setRecvMessageCallback(
    [this](const trantor::TcpConnectionPtr &conn,
           trantor::MsgBuffer *buffer) { onMessage(conn, buffer); });
  /* Must be called before start(); the unit is seconds. Catches fully-idle
   * connections; the sweep timer below catches dribbling ones. */
  m_server->kickoffIdleConnections(kIdleTimeoutS);

  loop->runInLoop([this, loop]() {
    loop->runEvery(kSweepIntervalS, [this]() { sweepStalledRequests(); });
  });

  m_server->start();
  /* TcpServer::start() only queues its listen() onto the loop; wait for
   * that queued work to run so callers (and the first kubelet probe) never
   * see ECONNREFUSED from a Start() that already returned success. */
  {
    std::promise<void> listening;
    loop->runInLoop([&listening]() { listening.set_value(); });
    listening.get_future().wait();
  }
  m_started = true;
  rdrs_logger::info(
    "Probe listener (ping/health, no auth) running on " + ip + ":" +
    std::to_string(port));
  return true;
}

void ProbeServer::Stop() {
  if (!m_started) {
    m_loop_thread.reset();
    return;
  }
  m_started = false;
  /* stop() closes the acceptor and every connection through the loop and
   * waits for that to complete; the EventLoopThread destructor then quits
   * the loop and joins the thread. */
  m_server->stop();
  m_server.reset();
  m_loop_thread.reset();
  rdrs_logger::info("Probe listener stopped");
}

void ProbeServer::onConnection(const trantor::TcpConnectionPtr &conn) {
  if (conn->connected()) {
    if (m_conns.size() >= kMaxConnections) {
      /* Beyond the cap nothing is served; a probe landing here fails fast
       * and retries on a fresh connection once load drops. */
      conn->forceClose();
      return;
    }
    conn->setTcpNoDelay(true);
    ConnState state;
    state.conn = conn;
    m_conns.emplace(conn.get(), std::move(state));
  } else {
    m_conns.erase(conn.get());
  }
}

void ProbeServer::onMessage(const trantor::TcpConnectionPtr &conn,
                            trantor::MsgBuffer *buffer) {
  auto it = m_conns.find(conn.get());
  if (it == m_conns.end()) {
    /* Over-cap connection that was force-closed on accept but had bytes in
     * flight. */
    buffer->retrieveAll();
    return;
  }
  ConnState &state = it->second;

  /* Trantor delivers one callback per read; a single callback may carry a
   * partial request, several pipelined requests, or both. Parse until no
   * complete header block remains - returning with a complete request
   * still buffered would hang it until the client sends more. */
  while (true) {
    size_t readable = buffer->readableBytes();
    if (readable == 0) {
      state.has_partial = false;
      return;
    }
    size_t headerEnd = findHeaderEnd(buffer->peek(), readable);
    if (headerEnd == std::string::npos) {
      if (readable > kMaxHeaderBytes) {
        conn->send(buildResponse(431, "", false, false));
        buffer->retrieveAll();
        state.has_partial = false;  // shutting down; nothing left to sweep
        conn->shutdown();
        return;
      }
      /* Incomplete: remember when it started so the sweep timer can
       * enforce the deadline. */
      if (!state.has_partial) {
        state.has_partial = true;
        state.partial_since = std::chrono::steady_clock::now();
      }
      return;
    }
    if (headerEnd > kMaxHeaderBytes) {
      /* A COMPLETE header block over the cap must be rejected too: the
       * incomplete-path check above alone would let a client that delivers
       * its oversized headers in one read bypass the limit entirely. */
      conn->send(buildResponse(431, "", false, false));
      buffer->retrieveAll();
      state.has_partial = false;  // shutting down; nothing left to sweep
      conn->shutdown();
      return;
    }

    ParsedRequest req;
    bool close_after = false;
    int status;
    std::string body;
    bool head = false;
    if (!parseRequest(buffer->peek(), headerEnd, &req)) {
      status = 400;
      body = "Bad Request";
      close_after = true;
    } else {
    /* HEAD is GET-without-a-body for EVERY outcome (400/404/405 included),
     * matching the main port; only an unparseable request leaves it unset. */
    head = (req.method == "HEAD");
    if (req.has_chunked || req.has_body) {
      /* GET/HEAD with a body: answer like the main port's controllers
       * ("request should be empty") but close instead of resynchronizing -
       * skipping an unread body safely is not worth the parser surface. */
      status = 400;
      body = "Request should be empty";
      close_after = true;
    } else if (req.target != PING_PATH && req.target != HEALTH_PATH) {
      status = 404;
      body = "Not Found";
      close_after = !req.keep_alive;
    } else if (req.method != "GET" && req.method != "HEAD") {
      status = 405;
      body = "Method Not Allowed";
      close_after = !req.keep_alive;
    } else {
      if (req.target == PING_PATH) {
        /* Same counters as the main port's endpoints: one ping/health
         * metric regardless of listener. */
        PingEndPointMetricsUpdater metricsUpdater;
        /* 503 until the main server actually listens, 200 forever after:
         * this makes the STARTUP probe usable on this port with the same
         * semantics as pinging the main port, while liveness (which
         * Kubernetes only runs once startup has passed) is untouched -
         * g_drogon_up is latched at startup and never clears during a
         * data-path stall, so a stall can still never fail this endpoint.
         * The gate makes /ping behave identically on both ports at all
         * times. */
        status = g_drogon_up.load(std::memory_order_acquire) ? 200 : 503;
      } else {
        HealthEndPointMetricsUpdater metricsUpdater;
        status = healthStatus();
        body = (status == 200) ? "1" : "0";
      }
      close_after = !req.keep_alive;
    }
    }

    /* parseRequest sets consumed to the full header block unconditionally,
     * so this also discards a malformed block before the close below. */
    buffer->retrieve(req.consumed);
    state.has_partial = false;
    conn->send(buildResponse(status, body, !close_after, head));
    if (close_after) {
      /* shutdown(), never forceClose(): forceClose drops the send buffer
       * and would truncate the response a kubelet is reading. */
      buffer->retrieveAll();
      conn->shutdown();
      return;
    }
  }
}

void ProbeServer::sweepStalledRequests() {
  const auto now = std::chrono::steady_clock::now();
  /* Collect first, close after: forceClose() runs the disconnect callback
   * synchronously on this same loop thread, and that callback erases from
   * m_conns - closing inside the loop would invalidate the iterator. */
  std::vector<std::shared_ptr<trantor::TcpConnection>> stalled;
  for (auto &entry : m_conns) {
    if (entry.second.has_partial &&
        now - entry.second.partial_since > kRequestDeadline) {
      stalled.push_back(entry.second.conn);
      entry.second.has_partial = false;
    }
  }
  for (auto &conn : stalled) {
    /* The response deadline has long passed for whatever this client is
     * dribbling; reclaim the connection. forceClose is correct here -
     * there is no response to flush. */
    conn->forceClose();
  }
}
