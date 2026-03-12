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

#ifndef MYSQL_CONN_H
#define MYSQL_CONN_H

#include "pink_conn.h"
#include "server_thread.h"
#include <string>
#include <mutex>

class MysqlConn : public pink::PinkConn {
public:
  MysqlConn(int fd, const std::string& ip_port,
            pink::Thread* thread, void* worker_data,
            const char* backend_host, uint16_t backend_port);
  ~MysqlConn() override;

  pink::ReadStatus GetRequest() override;
  pink::WriteStatus SendReply() override;

private:
  int backend_fd_;
  std::string request_buf_;
  std::string response_buf_;
  size_t write_pos_;
  bool handshake_done_;
  bool should_close_;
  uint32_t client_capabilities_;
  std::string backend_host_;
  uint16_t backend_port_;

  bool do_handshake();
  bool forward_and_relay();
  void send_err_to_client(const char* message);
};

class MysqlConnFactory : public pink::ConnFactory {
public:
  MysqlConnFactory(const char* backend_host, uint16_t backend_port);
  std::shared_ptr<pink::PinkConn> NewPinkConn(
      int connfd, const std::string& ip_port,
      pink::Thread* thread, void* worker_specific_data,
      pink::PinkEpoll* pink_epoll = nullptr) const override;
private:
  std::string backend_host_;
  uint16_t backend_port_;
};

class MysqlHandle : public pink::ServerHandle {
public:
  MysqlHandle() : counter_(0) {}
  int CreateWorkerSpecificData(void** data) const override;
private:
  mutable std::mutex mutex_;
  mutable int counter_;
};

#endif
