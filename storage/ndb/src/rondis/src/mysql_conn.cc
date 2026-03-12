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

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <unistd.h>

using namespace mysql_protocol;

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
                                   uint16_t backend_port)
    : backend_host_(backend_host), backend_port_(backend_port) {}

std::shared_ptr<pink::PinkConn> MysqlConnFactory::NewPinkConn(
    int connfd,
    const std::string& ip_port,
    pink::Thread* thread,
    void* worker_specific_data,
    [[maybe_unused]] pink::PinkEpoll* pink_epoll) const {
  return std::make_shared<MysqlConn>(connfd, ip_port, thread,
                                     worker_specific_data,
                                     backend_host_.c_str(),
                                     backend_port_);
}

// -----------------------------------------------------------------------
// MysqlConn
// -----------------------------------------------------------------------

MysqlConn::MysqlConn(int fd, const std::string& ip_port,
                     pink::Thread* thread, void* /*worker_data*/,
                     const char* backend_host, uint16_t backend_port)
    : PinkConn(fd, ip_port, thread),
      backend_fd_(-1),
      write_pos_(0),
      handshake_done_(false),
      should_close_(false),
      client_capabilities_(0),
      backend_host_(backend_host),
      backend_port_(backend_port) {
  if (!do_handshake()) {
    should_close_ = true;
  }
}

MysqlConn::~MysqlConn() {
  if (backend_fd_ >= 0) {
    close(backend_fd_);
    backend_fd_ = -1;
  }
}

void MysqlConn::send_err_to_client(const char* message) {
  std::string err_pkt;
  build_err_packet(err_pkt, 0, 2003, "HY000", message);
  write_all(fd(), err_pkt.data(), err_pkt.size());
}

bool MysqlConn::do_handshake() {
  // Connect to backend mysqld
  backend_fd_ = connect_to_backend(backend_host_.c_str(), backend_port_);
  if (backend_fd_ < 0) {
    send_err_to_client("MySQL router: cannot connect to backend mysqld");
    return false;
  }

  // Read server greeting from backend
  std::string handshake_pkt;
  if (!read_packet(backend_fd_, handshake_pkt)) {
    send_err_to_client("MySQL router: failed to read backend handshake");
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }

  // Relay server greeting to client
  if (!write_all(fd(), handshake_pkt.data(), handshake_pkt.size())) {
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }

  // Read client auth response
  std::string auth_response;
  if (!read_packet(fd(), auth_response)) {
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }

  // Extract client capabilities from auth response (first 4 bytes of payload)
  if (auth_response.size() >= HEADER_SIZE + 4) {
    const char* payload = auth_response.data() + HEADER_SIZE;
    client_capabilities_ =
        (uint32_t)(uint8_t)payload[0] |
        ((uint32_t)(uint8_t)payload[1] << 8) |
        ((uint32_t)(uint8_t)payload[2] << 16) |
        ((uint32_t)(uint8_t)payload[3] << 24);
  }

  // Forward auth response to backend
  if (!write_all(backend_fd_, auth_response.data(), auth_response.size())) {
    close(backend_fd_);
    backend_fd_ = -1;
    return false;
  }

  // Handle auth exchange: loop until we get OK or ERR from backend
  while (true) {
    std::string pkt;
    if (!read_packet(backend_fd_, pkt)) {
      close(backend_fd_);
      backend_fd_ = -1;
      return false;
    }

    uint32_t payload_len = packet_length(pkt.data());
    if (payload_len == 0) {
      // Empty packet — relay and continue
      write_all(fd(), pkt.data(), pkt.size());
      continue;
    }

    uint8_t marker = (uint8_t)pkt[HEADER_SIZE];

    if (marker == OK_MARKER || marker == ERR_MARKER) {
      // Final auth result — relay to client
      write_all(fd(), pkt.data(), pkt.size());
      if (marker == ERR_MARKER) {
        close(backend_fd_);
        backend_fd_ = -1;
        return false;
      }
      handshake_done_ = true;
      return true;
    }

    // Auth continuation (AuthSwitchRequest, AuthMoreData, etc.)
    // Relay to client and read client response
    if (!write_all(fd(), pkt.data(), pkt.size())) {
      close(backend_fd_);
      backend_fd_ = -1;
      return false;
    }

    std::string client_pkt;
    if (!read_packet(fd(), client_pkt)) {
      close(backend_fd_);
      backend_fd_ = -1;
      return false;
    }

    if (!write_all(backend_fd_, client_pkt.data(), client_pkt.size())) {
      close(backend_fd_);
      backend_fd_ = -1;
      return false;
    }
  }
}

pink::ReadStatus MysqlConn::GetRequest() {
  if (should_close_) {
    return pink::kReadClose;
  }

  if (!handshake_done_) {
    return pink::kReadClose;
  }

  // Read from client (non-blocking, epoll-triggered)
  char tmp_buf[16384];
  ssize_t n = read(fd(), tmp_buf, sizeof(tmp_buf));
  if (n == 0) {
    // Client closed connection
    if (backend_fd_ >= 0) {
      // Send COM_QUIT to backend
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

  // Extract command byte
  uint8_t cmd = (uint8_t)request_buf_[HEADER_SIZE];

  // Forward complete packet to backend (synchronous blocking I/O)
  if (backend_fd_ < 0 ||
      !write_all(backend_fd_, request_buf_.data(), total_pkt_size)) {
    // Remove the consumed packet
    request_buf_.erase(0, total_pkt_size);
    should_close_ = true;
    return pink::kReadClose;
  }

  // Remove the consumed packet (keep any trailing bytes for next packet)
  request_buf_.erase(0, total_pkt_size);

  if (cmd == COM_QUIT) {
    should_close_ = true;
    return pink::kReadClose;
  }

  // Read complete response from backend (synchronous blocking I/O)
  response_buf_.clear();
  write_pos_ = 0;
  if (!read_response(backend_fd_, response_buf_, client_capabilities_)) {
    should_close_ = true;
    return pink::kReadClose;
  }

  set_is_reply(true);
  return pink::kReadAll;
}

pink::WriteStatus MysqlConn::SendReply() {
  if (response_buf_.empty()) {
    return pink::kWriteAll;
  }

  ssize_t n = write(fd(), response_buf_.data() + write_pos_,
                    response_buf_.size() - write_pos_);
  if (n < 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return pink::kWriteHalf;
    }
    return pink::kWriteError;
  }
  write_pos_ += n;
  if (write_pos_ >= response_buf_.size()) {
    response_buf_.clear();
    write_pos_ = 0;
    return pink::kWriteAll;
  }
  return pink::kWriteHalf;
}
