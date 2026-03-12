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

// Callback type for executing a RonSQL query.
// Parameters: query, query_len, database, thread_index, result_out, error_out
// Returns true on success (result in result_out as TSV), false on error.
typedef bool (*MysqlRouterRonSQLHandler)(
    const char* query, size_t query_len,
    const char* database, int thread_index,
    std::string& result_out,
    std::string& error_out);

// Global RonSQL handler — set by main.cc before starting the router.
extern MysqlRouterRonSQLHandler g_mysql_ronsql_handler;

class MysqlConn : public pink::PinkConn {
public:
  MysqlConn(int fd, const std::string& ip_port,
            pink::Thread* thread, void* worker_data,
            const char* backend_host, uint16_t backend_port,
            int thread_id_offset);
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
  int worker_id_;
  std::string current_database_;

  bool send_server_greeting();
  bool complete_auth();
  void send_err_to_client(const char* message);
  bool try_ronsql(const char* query, size_t query_len, uint8_t seq);
  bool is_select_query(const char* query, size_t query_len);
  void track_database_change(uint8_t cmd, const char* payload, size_t len);
};

class MysqlConnFactory : public pink::ConnFactory {
public:
  MysqlConnFactory(const char* backend_host, uint16_t backend_port,
                   int thread_id_offset);
  std::shared_ptr<pink::PinkConn> NewPinkConn(
      int connfd, const std::string& ip_port,
      pink::Thread* thread, void* worker_specific_data,
      pink::PinkEpoll* pink_epoll = nullptr) const override;
private:
  std::string backend_host_;
  uint16_t backend_port_;
  int thread_id_offset_;
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
