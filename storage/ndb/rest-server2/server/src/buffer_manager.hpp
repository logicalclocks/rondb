/*
 * Copyright (C) 2023 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_

#include "rdrs_dal.h"
#include "config_structs.hpp"
#include "rdrs_const.h"

#include <cstdint>
#include <memory>
#include <cstring>
#include <mutex>
#include <vector>

struct MemoryStats {
  int64_t allocationsCount;
  int64_t deallocationsCount;
  int64_t buffersCount;
  int64_t freeBuffers;
};

class RS_BufferArrayManager {
 public:
  RS_BufferArrayManager() {
    if (ADDRESS_SIZE != 4) {
      throw std::runtime_error("Address size must be 4");
    }
    if (globalConfigs.internal.reqBufferSize % ADDRESS_SIZE != 0 ||
        globalConfigs.internal.respBufferSize % ADDRESS_SIZE != 0) {
      throw std::runtime_error("Buffer size must be a multiple of 4");
    }

    int64_t preAllocateBuffers = globalConfigs.internal.preAllocatedBuffers;
    reqBuffersStats            = {preAllocateBuffers, 0, preAllocateBuffers, 0};
    respBuffersStats           = {preAllocateBuffers, 0, preAllocateBuffers, 0};

    for (int i = 0; i < preAllocateBuffers; i++) {
      reqBufferArray.push_back(allocate_req_buffer());
      respBufferArray.push_back(allocate_resp_buffer());
    }
  }

  RS_BufferArrayManager(const RS_BufferArrayManager &)            = delete;
  RS_BufferArrayManager &operator=(const RS_BufferArrayManager &) = delete;
  RS_BufferArrayManager(RS_BufferArrayManager &&)                 = delete;
  RS_BufferArrayManager &operator=(RS_BufferArrayManager &&)      = delete;

  ~RS_BufferArrayManager() {
    std::lock_guard<std::mutex> reqLock(reqBufferMutex);
    std::lock_guard<std::mutex> respLock(respBufferMutex);

    for (auto &buffer : reqBufferArray) {
      delete[] buffer.buffer;
    }
    for (auto &buffer : respBufferArray) {
      delete[] buffer.buffer;
    }
  }

  static RS_Buffer allocate_req_buffer() {
    auto reqBufferSize = globalConfigs.internal.reqBufferSize;
    auto buff          = RS_Buffer();
    buff.buffer        = new char[reqBufferSize];
    buff.size          = reqBufferSize;
    return buff;
  }

  static RS_Buffer allocate_resp_buffer() {
    auto respBufferSize = globalConfigs.internal.respBufferSize;
    auto buff           = RS_Buffer();
    buff.buffer         = new char[respBufferSize];
    buff.size           = respBufferSize;
    return buff;
  }

  RS_Buffer get_req_buffer() {
    std::lock_guard<std::mutex> lock(reqBufferMutex);
    auto numBuffersLeft = reqBufferArray.size();
    if (numBuffersLeft > 0) {
      auto buffer = reqBufferArray[numBuffersLeft - 1];
      reqBufferArray.pop_back();
      return buffer;
    }
    auto buffer = allocate_req_buffer();
    reqBuffersStats.buffersCount++;
    reqBuffersStats.allocationsCount++;
    return buffer;
  }

  RS_Buffer get_resp_buffer() {
    std::lock_guard<std::mutex> lock(respBufferMutex);
    auto numBuffersLeft = respBufferArray.size();
    if (numBuffersLeft > 0) {
      auto buffer = respBufferArray[numBuffersLeft - 1];
      respBufferArray.pop_back();
      return buffer;
    }
    auto buffer = allocate_resp_buffer();
    respBuffersStats.buffersCount++;
    respBuffersStats.allocationsCount++;
    return buffer;
  }

  void return_req_buffer(RS_Buffer buffer) {
    std::lock_guard<std::mutex> lock(reqBufferMutex);
    reqBufferArray.push_back(buffer);
  }

  void return_resp_buffer(RS_Buffer buffer) {
    std::lock_guard<std::mutex> lock(respBufferMutex);
    respBufferArray.push_back(buffer);
  }

  MemoryStats get_req_buffers_stats() {
    // update the free buffers count
    std::lock_guard<std::mutex> lock(reqBufferMutex);
    reqBuffersStats.freeBuffers = static_cast<int64_t>(reqBufferArray.size());
    return reqBuffersStats;
  }

  MemoryStats get_resp_buffers_stats() {
    // update the free buffers count
    std::lock_guard<std::mutex> lock(respBufferMutex);
    respBuffersStats.freeBuffers = static_cast<int64_t>(respBufferArray.size());
    return respBuffersStats;
  }

 private:
  std::vector<RS_Buffer> reqBufferArray;
  std::vector<RS_Buffer> respBufferArray;
  MemoryStats reqBuffersStats{};
  MemoryStats respBuffersStats{};
  std::mutex reqBufferMutex;
  std::mutex respBufferMutex;
};

#define EN_STATUS_MSG_LEN 256
struct EN_Status {
  uint32_t retValue;
  HTTP_CODE http_code;                // 200 for successful operation
  char message[EN_STATUS_MSG_LEN]{};  // error message.
  EN_Status() : retValue(0), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  explicit EN_Status(HTTP_CODE http_code) : retValue(0), http_code(http_code), message("") {
  }
  explicit EN_Status(uint32_t retValue)
      : retValue(retValue), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  EN_Status(HTTP_CODE http_code, const char *message) : retValue(0), http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
  EN_Status(HTTP_CODE http_code, uint32_t retValue)
      : retValue(retValue), http_code(http_code), message("") {
  }
  EN_Status(HTTP_CODE http_code, uint32_t retValue, const char *message)
      : retValue(retValue), http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
};

extern RS_BufferArrayManager rsBufferArrayManager;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_
