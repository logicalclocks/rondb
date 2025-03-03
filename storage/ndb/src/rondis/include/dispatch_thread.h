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

#ifndef PINK_SRC_DISPATCH_THREAD_H_
#define PINK_SRC_DISPATCH_THREAD_H_

#include <set>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "debug.h"
#include "server_thread.h"
#include "pink_conn.h"

namespace pink {

class PinkItem;
struct PinkFiredEvent;
class WorkerThread;

class DispatchThread : public ServerThread {
 public:
  DispatchThread(int port,
                 int work_num,
                 ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::string &ip,
                 int port,
                 int work_num,
                 ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);
  DispatchThread(const std::set<std::string>& ips,
                 int port,
                 int work_num,
                 ConnFactory* conn_factory,
                 int cron_interval,
                 int queue_limit,
                 const ServerHandle* handle);

  virtual ~DispatchThread() override;

  virtual int StartThread() override;

  virtual int StopThread() override;

  virtual void set_keepalive_timeout(int timeout) override;

  virtual int conn_num() const override;

  virtual std::vector<ServerThread::ConnInfo> conns_info() const override;

  virtual std::shared_ptr<PinkConn> MoveConnOut(int fd) override;

  virtual void MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& type) override;

  virtual void KillAllConns() override;

  virtual bool KillConn(const std::string& ip_port) override;

  void HandleNewConn(const int connfd, const std::string& ip_port) override;

  void SetQueueLimit(int queue_limit) override;
 private:
  /*
   * Here we used auto poll to find the next work thread,
   * last_thread_ is the last work thread
   */
  int last_thread_;
  int work_num_;
  /*
   * This is the work threads
   */
  WorkerThread** worker_thread_;
  int queue_limit_;
  std::map<WorkerThread*, void*> localdata_;

  void HandleConnEvent(PinkFiredEvent *pfe) override {
    UNUSED(pfe);
  }

  // No copying allowed
  DispatchThread(const DispatchThread&);
  void operator=(const DispatchThread&);
};  // class DispatchThread

}  // namespace pink
#endif  // PINK_SRC_DISPATCH_THREAD_H_
