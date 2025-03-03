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

#ifndef PINK_INCLUDE_REDIS_CONN_H_
#define PINK_INCLUDE_REDIS_CONN_H_

#include <map>
#include <vector>
#include <string>

#include "pink_define.h"
#include "pink_conn.h"
#include "redis_parser.h"

namespace pink {

typedef std::vector<std::string> RedisCmdArgsType;

enum HandleType {
  kSynchronous,
  kAsynchronous
};

class RedisConn: public PinkConn {
 public:
  RedisConn(const int fd,
            const std::string& ip_port,
            Thread* thread,
            PinkEpoll* pink_epoll = nullptr,
            const HandleType& handle_type = kSynchronous,
            const int rbuf_max_len = REDIS_MAX_MESSAGE);
  virtual ~RedisConn() override;

  virtual ReadStatus GetRequest() override;
  virtual WriteStatus SendReply() override;
  virtual int WriteResp(const std::string& resp) override;

  void TryResizeBuffer() override;
  void SetHandleType([[maybe_unused]]/*todo remove?*/
                     const HandleType& handle_type);
  HandleType GetHandleType();

  virtual void ProcessRedisCmds(const std::vector<RedisCmdArgsType>& argvs, bool async, std::string* response);
  void NotifyEpoll(bool success);

  virtual int DealMessage(const RedisCmdArgsType& argv, std::string* response) = 0;

 private:
  static int ParserDealMessageCb(RedisParser* parser, const RedisCmdArgsType& argv);
  static int ParserCompleteCb(RedisParser* parser, const std::vector<RedisCmdArgsType>& argvs);
  ReadStatus ParseRedisParserStatus(RedisParserStatus status);

  HandleType handle_type_;

  char* rbuf_;
  int rbuf_len_;
  int rbuf_max_len_;
  int msg_peak_;
  int command_len_;

  uint32_t wbuf_pos_;
  std::string response_;

  // For Redis Protocol parser
  int last_read_pos_;
  RedisParser redis_parser_;
  long bulk_len_;
};

}  // namespace pink
#endif  // PINK_INCLUDE_REDIS_CONN_H_
