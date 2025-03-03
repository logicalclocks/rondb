// Copyright (c) 2015-present, Qihoo, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

/*
   Copyright (c) 2024, 2025, Hopsworks and/or its affiliates.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef PINK_INCLUDE_SERVER_THREAD_H_
#define PINK_INCLUDE_SERVER_THREAD_H_

#include <set>
#include <vector>
#include <memory>
#include <string>
#include <mutex>

#ifdef __ENABLE_SSL
#include <openssl/ssl.h>
#include <openssl/conf.h>
#include <openssl/err.h>
#endif

#include "pink_define.h"
#include "pink_thread.h"

// remove 'unused parameter' warning
#define UNUSED(expr) do { (void)(expr); } while (0)

namespace pink {

class ServerSocket;
class PinkEpoll;
class PinkConn;
struct PinkFiredEvent;
class ConnFactory;
class WorkerThread;

/*
 *  ServerHandle will be invoked at appropriate occasion
 *  in server thread's main loop.
 */
class ServerHandle {
 public:
  ServerHandle() {}
  virtual ~ServerHandle() {}

  /*
   *  CronHandle() will be invoked on every cron_interval elapsed.
   */
  virtual void CronHandle() const {}

  /*
   *  FdTimeoutHandle(...) will be invoked after connection timeout.
   */
  virtual void FdTimeoutHandle(int fd, const std::string& ip_port) const {
    UNUSED(fd);
    UNUSED(ip_port);
  }

  /*
   *  FdClosedHandle(...) will be invoked before connection closed.
   */
  virtual void FdClosedHandle(int fd, const std::string& ip_port) const {
    UNUSED(fd);
    UNUSED(ip_port);
  }

  /*
   *  AccessHandle(...) will be invoked after client fd accept()
   *  but before handled.
   */
  virtual bool AccessHandle(std::string& ip) const {
    UNUSED(ip);
    return true;
  }

  virtual bool AccessHandle(int fd, std::string& ip) const {
    UNUSED(fd);
    UNUSED(ip);
    return true;
  }

  /*
   *  CreateWorkerSpecificData(...) will be invoked in StartThread() routine.
   *  'data' pointer should be assigned, we will pass the pointer as parameter
   *  in every connection's factory create function.
   */
  virtual int CreateWorkerSpecificData(void** data) const {
    UNUSED(data);
    return 0;
  }

  /*
   *  DeleteWorkerSpecificData(...) is related to CreateWorkerSpecificData(...),
   *  it will be invoked in StopThread(...) routine,
   *  resources assigned in CreateWorkerSpecificData(...) should be deleted in
   *  this handle
   */
  virtual int DeleteWorkerSpecificData(void* data) const {
    UNUSED(data);
    return 0;
  }
};

const char kKillAllConnsTask[] = "kill_all_conns";

const int kDefaultKeepAliveTime = 60;  // (s)

class ServerThread : public Thread {
 public:
  ServerThread(int port, int cron_interval, const ServerHandle *handle);
  ServerThread(const std::string& bind_ip, int port, int cron_interval,
               const ServerHandle *handle);
  ServerThread(const std::set<std::string>& bind_ips, int port,
               int cron_interval, const ServerHandle *handle);

#ifdef __ENABLE_SSL
  /*
   * Enable TLS, set before StartThread, default: false
   * Just HTTPConn has supported for now.
   */
  int EnableSecurity(const std::string& cert_file, const std::string& key_file);
  SSL_CTX* ssl_ctx() { return ssl_ctx_; }
  bool security() { return security_; }
#endif

  int SetTcpNoDelay(int connfd);

  /*
   * StartThread will return the error code as pthread_create
   * Return 0 if success
   */
  virtual int StartThread() override;

  virtual void set_keepalive_timeout(int timeout) = 0;

  virtual int conn_num() const = 0;

  struct ConnInfo {
    int fd;
    std::string ip_port;
    struct timeval last_interaction;
  };
  virtual std::vector<ConnInfo> conns_info() const = 0;

  // Move out from server thread
  virtual std::shared_ptr<PinkConn> MoveConnOut(int fd) = 0;
  // Move into server thread
  virtual void MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& type) = 0;

  virtual void KillAllConns() = 0;
  virtual bool KillConn(const std::string& ip_port) = 0;

  virtual void HandleNewConn(int connfd, const std::string& ip_port) = 0;

  virtual void SetQueueLimit([[maybe_unused]]/*todo remove?*/
                             int queue_limit) { }

  virtual ~ServerThread() override;

 protected:
  /*
   * The Epoll event handler
   */
  PinkEpoll *pink_epoll_;

 private:
  friend class HolyThread;
  friend class DispatchThread;
  friend class WorkerThread;

  int cron_interval_;
  virtual void DoCronTask();

  // process events in epoll notify_queue
  virtual void ProcessNotifyEvents(const PinkFiredEvent* pfe);

  const ServerHandle *handle_;
  bool own_handle_;

#ifdef __ENABLE_SSL
  bool security_;
  SSL_CTX* ssl_ctx_;
#endif

  /*
   * The tcp server port and address
   */
  int port_;
  std::set<std::string> ips_;
  std::vector<ServerSocket*> server_sockets_;
  std::set<int32_t> server_fds_;

  virtual int InitHandle();
  virtual void *ThreadMain() override;
  /*
   * The server event handle
   */
  virtual void HandleConnEvent(PinkFiredEvent *pfe) = 0;
};

// !!!Attention: If u use this constructor, the keepalive_timeout_ will
// be equal to kDefaultKeepAliveTime(60s). In master-slave mode, the slave
// binlog receiver will close the binlog sync connection in HolyThread::DoCronTask
// if master did not send data in kDefaultKeepAliveTime.
extern ServerThread *NewHolyThread(
    int port,
    ConnFactory *conn_factory,
    int cron_interval = 0,
    const ServerHandle* handle = nullptr);
extern ServerThread *NewHolyThread(
    const std::string &bind_ip, int port,
    ConnFactory *conn_factory,
    int cron_interval = 0,
    const ServerHandle* handle = nullptr);
extern ServerThread *NewHolyThread(
    const std::set<std::string>& bind_ips, int port,
    ConnFactory *conn_factory,
    int cron_interval = 0,
    const ServerHandle* handle = nullptr);
extern ServerThread *NewHolyThread(
    const std::set<std::string>& bind_ips, int port,
    ConnFactory *conn_factory, bool async,
    int cron_interval = 0,
    const ServerHandle* handle = nullptr);

/**
 * This type Dispatch thread just get Connection and then Dispatch the fd to
 * worker thread
 *
 * @param port          the port number
 * @param conn_factory  connection factory object
 * @param cron_interval the cron job interval
 * @param queue_limit   the size limit of workers' connection queue
 * @param handle        the server's handle (e.g. CronHandle, AccessHandle...)
 */
extern ServerThread *NewDispatchThread(
    int port,
    int work_num, ConnFactory* conn_factory,
    int cron_interval = 0, int queue_limit = 1000,
    const ServerHandle* handle = nullptr);
extern ServerThread *NewDispatchThread(
    const std::string &ip, int port,
    int work_num, ConnFactory* conn_factory,
    int cron_interval = 0, int queue_limit = 1000,
    const ServerHandle* handle = nullptr);
extern ServerThread *NewDispatchThread(
    const std::set<std::string>& ips, int port,
    int work_num, ConnFactory* conn_factory,
    int cron_interval = 0, int queue_limit = 1000,
    const ServerHandle* handle = nullptr);

}  // namespace pink
#endif  // PINK_INCLUDE_SERVER_THREAD_H_
