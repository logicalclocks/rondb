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

#ifndef PINK_SRC_WORKER_THREAD_H_
#define PINK_SRC_WORKER_THREAD_H_

#include <string>
#include <functional>
#include <map>
#include <atomic>
#include <vector>
#include <set>
#include <mutex>

#include "debug.h"

#include "server_thread.h"
#include "pink_epoll.h"
#include "pink_thread.h"
#include "pink_define.h"

namespace pink {

class PinkItem;
class PinkEpoll;
struct PinkFiredEvent;
class PinkConn;
class ConnFactory;

class WorkerThread : public Thread {
 public:
  explicit WorkerThread(ConnFactory *conn_factory, ServerThread* server_thread,
                        int queue_limit_, int cron_interval = 0);

  virtual ~WorkerThread() override;

  void set_keepalive_timeout(int timeout) {
    keepalive_timeout_ = timeout;
  }

  int conn_num() const;

  std::vector<ServerThread::ConnInfo> conns_info() const;

  std::shared_ptr<PinkConn> MoveConnOut(int fd);

  bool MoveConnIn(std::shared_ptr<PinkConn> conn, const NotifyType& notify_type, bool force);

  bool MoveConnIn(const PinkItem& it, bool force);

  PinkEpoll* pink_epoll() {
    return pink_epoll_;
  }
  bool TryKillConn(const std::string& ip_port);

  mutable std::mutex wlock_; /* For external statistics */
  std::map<int, std::shared_ptr<PinkConn>> conns_;

  void* private_data_;

 private:
  ServerThread* server_thread_;
  ConnFactory *conn_factory_;
  int cron_interval_;


  /*
   * The epoll handler
   */
  PinkEpoll *pink_epoll_;

  std::atomic<int> keepalive_timeout_;  // keepalive second

  virtual void *ThreadMain() override;
  void DoCronTask();

  mutable std::mutex killer_mutex_;
  std::set<std::string> deleting_conn_ipport_;

  // clean conns
  void CloseFd(std::shared_ptr<PinkConn> conn);
  void Cleanup();
};  // class WorkerThread

}  // namespace pink
#endif  // PINK_SRC_WORKER_THREAD_H_
