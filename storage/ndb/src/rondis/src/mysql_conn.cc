/*
 * Copyright (c) 2026, 2026, Hopsworks and/or its affiliates.
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

#include "mysql_conn.h"
#include "mysql_protocol.h"

#include <cctype>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <strings.h>
#include <unistd.h>

using namespace mysql_protocol;

// Global RonSQL handler — set by main.cc
MysqlRouterRonSQLHandler g_mysql_ronsql_handler = nullptr;

// -----------------------------------------------------------------------
// MysqlHandle
// -----------------------------------------------------------------------

int MysqlHandle::CreateWorkerSpecificData(void** data) const {
  std::lock_guard<std::mutex> lock(mutex_);
  *data = new int(counter_++);
  return 0;
}

// -----------------------------------------------------------------------
// MysqlConnFactory
// -----------------------------------------------------------------------

MysqlConnFactory::MysqlConnFactory(const char* backend_host,
                                   uint16_t backend_port,
                                   int thread_id_offset,
                                   bool debug_logging,
                                   const char* tls_cert_file,
                                   const char* tls_key_file)
    : backend_host_(backend_host), backend_port_(backend_port),
      thread_id_offset_(thread_id_offset), debug_logging_(debug_logging),
      ssl_ctx_(nullptr) {
  if (tls_cert_file && tls_key_file &&
      tls_cert_file[0] != '\0' && tls_key_file[0] != '\0') {
    ssl_ctx_ = create_server_ssl_ctx(tls_cert_file, tls_key_file);
    if (ssl_ctx_) {
      printf("MySQL Router TLS enabled\n");
    } else {
      fprintf(stderr, "MySQL Router TLS setup failed, running without TLS\n");
    }
  }
}

MysqlConnFactory::~MysqlConnFactory() {
  if (ssl_ctx_) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }
}

std::shared_ptr<pink::PinkConn> MysqlConnFactory::NewPinkConn(
    int connfd,
    const std::string& ip_port,
    pink::Thread* thread,
    void* worker_specific_data,
    [[maybe_unused]] pink::PinkEpoll* pink_epoll) const {
  return std::make_shared<MysqlConn>(connfd, ip_port, thread,
                                     worker_specific_data,
                                     backend_host_.c_str(),
                                     backend_port_,
                                     thread_id_offset_,
                                     debug_logging_,
                                     ssl_ctx_);
}

// -----------------------------------------------------------------------
// MysqlConn
// -----------------------------------------------------------------------

MysqlConn::MysqlConn(int fd, const std::string& ip_port,
                     pink::Thread* thread, void* worker_data,
                     const char* backend_host, uint16_t backend_port,
                     int thread_id_offset, bool debug_logging,
                     SSL_CTX* ssl_ctx)
    : PinkConn(fd, ip_port, thread),
      backend_fd_(-1),
      write_pos_(0),
      handshake_done_(false),
      should_close_(false),
      in_transaction_(false),
      client_capabilities_(0),
      backend_host_(backend_host),
      backend_port_(backend_port),
      worker_id_(thread_id_offset),
      ssl_ctx_(ssl_ctx),
      client_ssl_(nullptr),
      debug_logging_(debug_logging) {
  if (worker_data != nullptr) {
    worker_id_ = thread_id_offset + *static_cast<int*>(worker_data);
  }
  // Phase 1 of handshake: connect to backend and send server greeting to
  // client. The client will then send its auth response, which triggers
  // Pink's epoll → GetRequest() where we complete the auth exchange.
  if (!send_server_greeting()) {
    should_close_ = true;
  }
}

MysqlConn::~MysqlConn() {
  if (client_ssl_) {
    SSL_shutdown(client_ssl_);
    SSL_free(client_ssl_);
    client_ssl_ = nullptr;
  }
  if (backend_fd_ >= 0) {
    close(backend_fd_);
    backend_fd_ = -1;
  }
}

bool MysqlConn::client_read_packet(std::string& out) {
  return client_ssl_ ? ssl_read_packet(client_ssl_, out)
                     : read_packet(fd(), out);
}

bool MysqlConn::client_write_all(const char* data, size_t len) {
  return client_ssl_ ? ssl_write_all(client_ssl_, data, len)
                     : write_all(fd(), data, len);
}

void MysqlConn::send_err_to_client(const char* message) {
  std::string err_pkt;
  build_err_packet(err_pkt, 0, 2003, "HY000", message);
  write_all(fd(), err_pkt.data(), err_pkt.size());
}

bool MysqlConn::send_server_greeting() {
  // Phase 1: connect to backend and relay the server greeting to the client.
  // Called from constructor before Pink registers the fd with epoll.
  // After receiving the greeting, the client will send its auth response,
  // which triggers epoll → GetRequest() → complete_auth().
  backend_fd_ = connect_to_backend(backend_host_.c_str(), backend_port_);
  if (backend_fd_ < 0) {
    send_err_to_client("MySQL router: cannot connect to backend mysqld");
    return false;
  }

  std::string handshake_pkt;
  if (!read_packet(backend_fd_, handshake_pkt)) {
    send_err_to_client("MySQL router: failed to read backend handshake");
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }

  // When TLS is not configured, strip CLIENT_SSL from the greeting so
  // clients don't attempt TLS upgrade. When TLS is configured, keep it
  // so clients can optionally upgrade.
  if (!ssl_ctx_) {
    const char* payload = handshake_pkt.data() + HEADER_SIZE;
    size_t payload_len = handshake_pkt.size() - HEADER_SIZE;
    // Find end of server_version (NUL-terminated, starts at offset 1)
    size_t ver_end = 1;
    while (ver_end < payload_len && payload[ver_end] != '\0') ver_end++;
    // capability_flags_lower at ver_end + 1(NUL) + 4(thread_id) +
    // 8(auth_data_1) + 1(filler) = ver_end + 14
    size_t caps_lower_offset = HEADER_SIZE + ver_end + 14;
    if (caps_lower_offset + 1 < handshake_pkt.size()) {
      uint16_t caps_lower =
          (uint16_t)(uint8_t)handshake_pkt[caps_lower_offset] |
          ((uint16_t)(uint8_t)handshake_pkt[caps_lower_offset + 1] << 8);
      caps_lower &= ~(uint16_t)(CLIENT_SSL & 0xFFFF);
      handshake_pkt[caps_lower_offset] = (char)(caps_lower & 0xFF);
      handshake_pkt[caps_lower_offset + 1] = (char)((caps_lower >> 8) & 0xFF);
    }
  }

  if (!write_all(fd(), handshake_pkt.data(), handshake_pkt.size())) {
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }
  return true;
}

bool MysqlConn::complete_auth() {
  // Phase 2: complete the auth exchange. Called from GetRequest() when the
  // client's auth response arrives (epoll-triggered). Uses blocking I/O on
  // both fds — read_exact/write_all handle EAGAIN for non-blocking client fd.

  // Read client's first packet (always plain TCP at this point).
  // This is either an SSLRequest or the full auth response.
  std::string auth_response;
  if (!read_packet(fd(), auth_response)) {
    return false;
  }

  // Extract client capabilities from first packet
  if (auth_response.size() >= HEADER_SIZE + 4) {
    const char* payload = auth_response.data() + HEADER_SIZE;
    client_capabilities_ =
        (uint32_t)(uint8_t)payload[0] |
        ((uint32_t)(uint8_t)payload[1] << 8) |
        ((uint32_t)(uint8_t)payload[2] << 16) |
        ((uint32_t)(uint8_t)payload[3] << 24);
  }

  // Check for SSLRequest: CLIENT_SSL set and payload is exactly 32 bytes
  // (capabilities(4) + max_packet_size(4) + charset(1) + reserved(23))
  uint32_t first_payload_len = packet_length(auth_response.data());
  bool is_ssl_request = (client_capabilities_ & CLIENT_SSL) &&
                         first_payload_len == 32 && ssl_ctx_;
  if (is_ssl_request) {
    // Client wants TLS — perform server-side TLS handshake
    client_ssl_ = ssl_accept(ssl_ctx_, fd());
    if (!client_ssl_) {
      send_err_to_client("MySQL router: TLS handshake failed");
      return false;
    }
    // Read the actual auth response over TLS
    auth_response.clear();
    if (!client_read_packet(auth_response)) {
      return false;
    }
    // Re-extract capabilities from the real auth response
    if (auth_response.size() >= HEADER_SIZE + 4) {
      const char* payload = auth_response.data() + HEADER_SIZE;
      client_capabilities_ =
          (uint32_t)(uint8_t)payload[0] |
          ((uint32_t)(uint8_t)payload[1] << 8) |
          ((uint32_t)(uint8_t)payload[2] << 16) |
          ((uint32_t)(uint8_t)payload[3] << 24);
    }
  }

  // Fix sequence number: if TLS upgrade happened, the auth response has
  // seq=2 (SSLRequest was seq=1), but backend expects seq=1.
  if (client_ssl_ && auth_response.size() >= HEADER_SIZE) {
    auth_response[3] = 1;  // Set sequence to 1
  }

  // Strip CLIENT_SSL from auth before forwarding to backend (plain connection)
  if (auth_response.size() >= HEADER_SIZE + 4) {
    size_t off = HEADER_SIZE;
    uint32_t caps =
        (uint32_t)(uint8_t)auth_response[off] |
        ((uint32_t)(uint8_t)auth_response[off + 1] << 8) |
        ((uint32_t)(uint8_t)auth_response[off + 2] << 16) |
        ((uint32_t)(uint8_t)auth_response[off + 3] << 24);
    caps &= ~CLIENT_SSL;
    auth_response[off] = (char)(caps & 0xFF);
    auth_response[off + 1] = (char)((caps >> 8) & 0xFF);
    auth_response[off + 2] = (char)((caps >> 16) & 0xFF);
    auth_response[off + 3] = (char)((caps >> 24) & 0xFF);
  }

  // Extract default database from auth response (HandshakeResponse41):
  // After capabilities(4) + max_packet_size(4) + charset(1) + reserved(23)
  // = offset 32 from payload start, then username (NUL-terminated),
  // then auth data, then database (NUL-terminated, if CLIENT_CONNECT_WITH_DB)
  static constexpr uint32_t CLIENT_CONNECT_WITH_DB = (1UL << 3);
  if ((client_capabilities_ & CLIENT_CONNECT_WITH_DB) &&
      auth_response.size() > HEADER_SIZE + 32) {
    const char* p = auth_response.data() + HEADER_SIZE + 32;
    const char* end = auth_response.data() + auth_response.size();
    while (p < end && *p != '\0') p++;
    if (p < end) p++;
    if (p < end) {
      static constexpr uint32_t CLIENT_PLUGIN_AUTH_LENENC = (1UL << 21);
      static constexpr uint32_t CLIENT_SECURE_CONNECTION = (1UL << 15);
      if (client_capabilities_ & CLIENT_PLUGIN_AUTH_LENENC) {
        const char* tmp = p;
        uint64_t auth_len = read_lenenc_int(tmp, end);
        p = tmp + auth_len;
      } else if (client_capabilities_ & CLIENT_SECURE_CONNECTION) {
        if (p < end) {
          uint8_t auth_len = (uint8_t)*p++;
          p += auth_len;
        }
      } else {
        while (p < end && *p != '\0') p++;
        if (p < end) p++;
      }
      if (p < end) {
        const char* db_start = p;
        while (p < end && *p != '\0') p++;
        if (p > db_start) {
          current_database_.assign(db_start, p - db_start);
        }
      }
    }
  }

  // Forward auth response to backend
  if (!write_all(backend_fd_, auth_response.data(), auth_response.size())) {
    return false;
  }

  // Handle auth exchange: loop until we get OK or ERR from backend.
  // Client I/O uses client_read_packet/client_write_all for TLS support.
  // When TLS is active, the backend never saw the SSLRequest (seq=1), so
  // backend seq numbers are off by 1 relative to the client's expectation.
  while (true) {
    std::string pkt;
    if (!read_packet(backend_fd_, pkt)) {
      return false;
    }

    // Fix seq: backend's seq is off by 1 when TLS is active
    // (backend didn't see the SSLRequest which consumed seq=1)
    if (client_ssl_ && pkt.size() >= HEADER_SIZE) {
      pkt[3] = (uint8_t)pkt[3] + 1;
    }

    uint32_t payload_len = packet_length(pkt.data());
    if (payload_len == 0) {
      client_write_all(pkt.data(), pkt.size());
      continue;
    }

    uint8_t marker = (uint8_t)pkt[HEADER_SIZE];

    if (marker == OK_MARKER || marker == ERR_MARKER) {
      if (!client_write_all(pkt.data(), pkt.size())) {
        return false;
      }
      if (marker == ERR_MARKER) {
        return false;
      }
      handshake_done_ = true;
      return true;
    }

    // Auth continuation (AuthSwitchRequest, AuthMoreData, etc.)
    if (!client_write_all(pkt.data(), pkt.size())) {
      return false;
    }

    std::string client_pkt;
    if (!client_read_packet(client_pkt)) {
      return false;
    }

    // Fix seq back: client's seq is 1 ahead of what backend expects
    if (client_ssl_ && client_pkt.size() >= HEADER_SIZE) {
      client_pkt[3] = (uint8_t)client_pkt[3] - 1;
    }

    if (!write_all(backend_fd_, client_pkt.data(), client_pkt.size())) {
      return false;
    }
  }
}

bool MysqlConn::is_select_query(const char* query, size_t query_len) {
  // Skip leading whitespace
  size_t i = 0;
  while (i < query_len && std::isspace((unsigned char)query[i])) i++;
  size_t remaining = query_len - i;
  if (remaining < 6) return false;
  // Case-insensitive check for "SELECT"
  return (strncasecmp(query + i, "SELECT", 6) == 0 &&
          (remaining == 6 || std::isspace((unsigned char)query[i + 6]) ||
           query[i + 6] == '('));
}

// Check if a word boundary exists at position i (before the keyword).
// A keyword must be preceded by whitespace, '(' or start-of-string.
static bool is_word_start(const char* query, size_t i) {
  if (i == 0) return true;
  char prev = query[i - 1];
  return std::isspace((unsigned char)prev) || prev == '(' || prev == ',';
}

// Check if a word boundary exists after a keyword of given length.
// A keyword must be followed by whitespace, '(' or end-of-string.
static bool is_word_end(const char* query, size_t pos, size_t kw_len,
                        size_t query_len) {
  size_t after = pos + kw_len;
  if (after >= query_len) return true;
  char next = query[after];
  return std::isspace((unsigned char)next) || next == '(';
}

// Quick scan for aggregate indicators: COUNT, SUM, AVG, MIN, MAX,
// GROUP BY. RonSQL only supports aggregate queries, so non-aggregate
// SELECTs should go straight to the proxy without trying RonSQL.
// This is a heuristic — false positives (e.g. column named "count")
// just cause a harmless RonSQL attempt that falls back to proxy.
static bool may_be_aggregate_query(const char* query, size_t query_len) {
  for (size_t i = 0; i + 2 < query_len; i++) {
    unsigned char c = (unsigned char)query[i];
    // Skip quoted strings (single and double quotes)
    if (c == '\'' || c == '"') {
      char quote = (char)c;
      i++;
      while (i < query_len && query[i] != quote) {
        if (query[i] == '\\') i++;  // skip escaped char
        i++;
      }
      continue;
    }
    // Skip backtick-quoted identifiers
    if (c == '`') {
      i++;
      while (i < query_len && query[i] != '`') i++;
      continue;
    }
    // Check for aggregate function names and GROUP BY
    if (!is_word_start(query, i)) continue;
    char upper = (char)std::toupper(c);
    if (upper == 'C' && i + 5 <= query_len &&
        strncasecmp(query + i, "COUNT", 5) == 0 &&
        is_word_end(query, i, 5, query_len)) {
      return true;
    }
    if (upper == 'S' && i + 3 <= query_len &&
        strncasecmp(query + i, "SUM", 3) == 0 &&
        is_word_end(query, i, 3, query_len)) {
      return true;
    }
    if (upper == 'A' && i + 3 <= query_len &&
        strncasecmp(query + i, "AVG", 3) == 0 &&
        is_word_end(query, i, 3, query_len)) {
      return true;
    }
    if (upper == 'M' && i + 3 <= query_len) {
      if (strncasecmp(query + i, "MIN", 3) == 0 &&
          is_word_end(query, i, 3, query_len)) {
        return true;
      }
      if (strncasecmp(query + i, "MAX", 3) == 0 &&
          is_word_end(query, i, 3, query_len)) {
        return true;
      }
    }
    if (upper == 'G' && i + 8 <= query_len &&
        strncasecmp(query + i, "GROUP BY", 8) == 0 &&
        is_word_start(query, i)) {
      return true;
    }
  }
  return false;
}

void MysqlConn::track_database_change(uint8_t cmd,
                                       const char* payload,
                                       size_t len) {
  if (cmd == COM_INIT_DB && len > 0) {
    current_database_.assign(payload, len);
    return;
  }
  if (cmd == COM_QUERY && len > 0) {
    // Check for "USE <database>" statement
    size_t i = 0;
    while (i < len && std::isspace((unsigned char)payload[i])) i++;
    if (i + 3 < len &&
        strncasecmp(payload + i, "USE", 3) == 0 &&
        std::isspace((unsigned char)payload[i + 3])) {
      i += 4;
      while (i < len && std::isspace((unsigned char)payload[i])) i++;
      // Extract database name (may be backtick-quoted)
      size_t db_start = i;
      if (i < len && payload[i] == '`') {
        db_start = ++i;
        while (i < len && payload[i] != '`') i++;
        current_database_.assign(payload + db_start, i - db_start);
      } else {
        while (i < len && !std::isspace((unsigned char)payload[i]) &&
               payload[i] != ';') i++;
        current_database_.assign(payload + db_start, i - db_start);
      }
    }
  }
}

void MysqlConn::update_transaction_state() {
  // Check the first packet in the response buffer for SERVER_STATUS_IN_TRANS.
  // This works for OK packets (after DML, SET, BEGIN, COMMIT, ROLLBACK) and
  // for the final OK/EOF packet in result sets (which also carries status flags).
  if (response_buf_.size() >= (size_t)HEADER_SIZE + 1) {
    uint16_t flags = extract_ok_status_flags(
        response_buf_.data(), response_buf_.size());
    in_transaction_ = (flags & SERVER_STATUS_IN_TRANS) != 0;
  }
}

static void log_route(const char* tag, const char* query, size_t query_len,
                      const char* reason = nullptr) {
  int print_len = (query_len > 200) ? 200 : (int)query_len;
  if (reason) {
    printf("MYROUTER %s: %.*s%s (reason: %s)\n",
           tag, print_len, query, (query_len > 200 ? "..." : ""), reason);
  } else {
    printf("MYROUTER %s: %.*s%s\n",
           tag, print_len, query, (query_len > 200 ? "..." : ""));
  }
}

bool MysqlConn::try_ronsql(const char* query, size_t query_len, uint8_t seq,
                            std::string& error_out) {
  if (g_mysql_ronsql_handler == nullptr) {
    error_out = "no handler";
    return false;
  }
  if (current_database_.empty()) {
    error_out = "no database selected";
    return false;
  }

  std::string result_tsv;
  std::string error_msg;

  bool ok = g_mysql_ronsql_handler(query, query_len,
                                   current_database_.c_str(),
                                   worker_id_,
                                   result_tsv, error_msg);
  if (!ok) {
    error_out = error_msg.empty() ? "handler returned false" : error_msg;
    return false;
  }

  // Convert TSV result to MySQL wire protocol result set
  response_buf_.clear();
  write_pos_ = 0;
  build_result_set_from_tsv(result_tsv, response_buf_, seq);
  return true;
}

pink::ReadStatus MysqlConn::GetRequest() {
  if (should_close_) {
    return pink::kReadClose;
  }

  if (!handshake_done_) {
    // First GetRequest() call — client sent auth response after receiving
    // the server greeting we sent in the constructor.
    if (!complete_auth()) {
      should_close_ = true;
      return pink::kReadClose;
    }
    return pink::kReadHalf;
  }

  // Read from client (non-blocking, epoll-triggered)
  char tmp_buf[16384];
  ssize_t n;
  if (client_ssl_) {
    n = SSL_read(client_ssl_, tmp_buf, sizeof(tmp_buf));
    if (n <= 0) {
      int err = SSL_get_error(client_ssl_, (int)n);
      if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        return pink::kReadHalf;
      }
      if (err == SSL_ERROR_ZERO_RETURN || n == 0) {
        if (backend_fd_ >= 0) {
          char quit_pkt[5] = {1, 0, 0, 0, (char)COM_QUIT};
          write_all(backend_fd_, quit_pkt, 5);
        }
        return pink::kReadClose;
      }
      return pink::kReadError;
    }
  } else {
    n = read(fd(), tmp_buf, sizeof(tmp_buf));
    if (n == 0) {
      if (backend_fd_ >= 0) {
        char quit_pkt[5] = {1, 0, 0, 0, (char)COM_QUIT};
        write_all(backend_fd_, quit_pkt, 5);
      }
      return pink::kReadClose;
    }
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return pink::kReadHalf;
      }
      return pink::kReadError;
    }
  }

  request_buf_.append(tmp_buf, n);

  // Check if we have a complete packet
  if (request_buf_.size() < (size_t)HEADER_SIZE) {
    return pink::kReadHalf;
  }
  uint32_t pkt_len = packet_length(request_buf_.data());
  size_t total_pkt_size = (size_t)HEADER_SIZE + pkt_len;
  if (request_buf_.size() < total_pkt_size) {
    return pink::kReadHalf;
  }

  // Extract command byte and sequence number
  uint8_t cmd = (uint8_t)request_buf_[HEADER_SIZE];
  uint8_t seq = (uint8_t)request_buf_[3];

  // Track database changes (USE, COM_INIT_DB) regardless of routing
  if (cmd == COM_INIT_DB || cmd == COM_QUERY) {
    const char* payload = request_buf_.data() + HEADER_SIZE + 1;
    size_t payload_len = pkt_len - 1;
    track_database_change(cmd, payload, payload_len);
  }

  // Try RonSQL for SELECT queries (only outside transactions)
  if (cmd == COM_QUERY && pkt_len > 1 && !in_transaction_) {
    const char* query = request_buf_.data() + HEADER_SIZE + 1;
    size_t query_len = pkt_len - 1;

    if (is_select_query(query, query_len)) {
      if (!may_be_aggregate_query(query, query_len)) {
        // Non-aggregate SELECT — skip RonSQL, go straight to proxy
        if (debug_logging_) {
          log_route("PROXY", query, query_len, "non-aggregate SELECT");
        }
      } else {
        std::string ronsql_error;
        // Response sequence number starts at 1 (after the request seq 0)
        if (try_ronsql(query, query_len, seq + 1, ronsql_error)) {
          // RonSQL handled it — consume packet and send response
          if (debug_logging_) {
            log_route("RONSQL", query, query_len);
          }
          request_buf_.erase(0, total_pkt_size);
          set_is_reply(true);
          return pink::kReadAll;
        }
        // RonSQL failed — fall through to backend proxy
        if (debug_logging_) {
          log_route("FALLBACK", query, query_len, ronsql_error.c_str());
        }
      }
    }
  }

  // Log COM_QUERY commands forwarded to backend
  if (debug_logging_ && cmd == COM_QUERY && pkt_len > 1) {
    const char* query = request_buf_.data() + HEADER_SIZE + 1;
    size_t query_len = pkt_len - 1;
    log_route("PROXY", query, query_len);
  }

  // Forward complete packet to backend (synchronous blocking I/O)
  if (backend_fd_ < 0 ||
      !write_all(backend_fd_, request_buf_.data(), total_pkt_size)) {
    request_buf_.erase(0, total_pkt_size);
    should_close_ = true;
    return pink::kReadClose;
  }

  request_buf_.erase(0, total_pkt_size);

  if (cmd == COM_QUIT) {
    should_close_ = true;
    return pink::kReadClose;
  }

  // COM_STMT_CLOSE has no response from the server
  if (cmd == COM_STMT_CLOSE) {
    set_is_reply(false);
    return pink::kReadHalf;
  }

  // Read complete response from backend (synchronous blocking I/O)
  response_buf_.clear();
  write_pos_ = 0;
  if (!read_response(backend_fd_, response_buf_, client_capabilities_, cmd)) {
    should_close_ = true;
    return pink::kReadClose;
  }

  // Handle LOCAL INFILE: backend asked client to send file data.
  // We must relay the file data from client to backend, then read the
  // final OK/ERR from backend. Uses SSL-aware client I/O.
  if (response_buf_.size() >= (size_t)HEADER_SIZE + 1 &&
      (uint8_t)response_buf_[HEADER_SIZE] == LOCAL_INFILE_MARKER) {
    // Send the LOCAL INFILE request to client
    if (!client_write_all(response_buf_.data(), response_buf_.size())) {
      should_close_ = true;
      return pink::kReadClose;
    }
    // Relay file data packets from client → backend until empty packet
    while (true) {
      std::string pkt;
      if (!client_read_packet(pkt)) {
        should_close_ = true;
        return pink::kReadClose;
      }
      if (!write_all(backend_fd_, pkt.data(), pkt.size())) {
        should_close_ = true;
        return pink::kReadClose;
      }
      if (packet_length(pkt.data()) == 0) break;
    }
    // Read final OK/ERR from backend
    response_buf_.clear();
    if (!read_packet(backend_fd_, response_buf_)) {
      should_close_ = true;
      return pink::kReadClose;
    }
    write_pos_ = 0;
  }

  // Reset router state on COM_RESET_CONNECTION / COM_CHANGE_USER
  if (cmd == COM_RESET_CONNECTION || cmd == COM_CHANGE_USER) {
    in_transaction_ = false;
    current_database_.clear();
  }

  // Update transaction state from backend OK packet status flags
  update_transaction_state();

  set_is_reply(true);
  return pink::kReadAll;
}

pink::WriteStatus MysqlConn::SendReply() {
  if (response_buf_.empty()) {
    return pink::kWriteAll;
  }

  ssize_t n;
  if (client_ssl_) {
    n = SSL_write(client_ssl_, response_buf_.data() + write_pos_,
                  (int)(response_buf_.size() - write_pos_));
    if (n <= 0) {
      int err = SSL_get_error(client_ssl_, (int)n);
      if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        return pink::kWriteHalf;
      }
      return pink::kWriteError;
    }
  } else {
    n = write(fd(), response_buf_.data() + write_pos_,
              response_buf_.size() - write_pos_);
    if (n < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        return pink::kWriteHalf;
      }
      return pink::kWriteError;
    }
  }
  write_pos_ += n;
  if (write_pos_ >= response_buf_.size()) {
    response_buf_.clear();
    write_pos_ = 0;
    return pink::kWriteAll;
  }
  return pink::kWriteHalf;
}
