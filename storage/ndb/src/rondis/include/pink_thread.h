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

#ifndef PINK_INCLUDE_PINK_THREAD_H_
#define PINK_INCLUDE_PINK_THREAD_H_

#include <pthread.h>
#include <string>
#include <atomic>
#include <mutex>

namespace pink {

class Thread {
 public:
  Thread();
  virtual ~Thread();

  virtual int StartThread();
  virtual int StopThread();
  int JoinThread();

  bool should_stop() {
    return should_stop_.load();
  }

  void set_should_stop() {
    should_stop_.store(true);
  }

  bool is_running() {
    return running_;
  }

  pthread_t thread_id() const {
    return thread_id_;
  }

  std::string thread_name() const {
    return thread_name_;
  }

  void set_thread_name(const std::string& name) {
    thread_name_ = name;
  }

 protected:
  std::atomic<bool> should_stop_;

 private:
  static void* RunThread(void* arg);
  virtual void *ThreadMain() = 0;

  std::mutex running_mu_;
  bool running_;
  pthread_t thread_id_;
  std::string thread_name_;

  /*
   * No allowed copy and copy assign
   */
  Thread(const Thread&);
  void operator=(const Thread&);
};

}  // namespace pink
#endif  // PINK_INCLUDE_PINK_THREAD_H_
