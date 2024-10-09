/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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
#include <NdbMutex.h>

#include <cstdint>
#include <memory>
#include <cstring>
#include <mutex>
#include <vector>

struct MemoryStats {
  Int64 allocationsCount;
  Int64 deallocationsCount;
  Int64 buffersCount;
  Int64 freeBuffers;
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
    Int64 preAllocateBuffers = globalConfigs.internal.preAllocatedBuffers;
    reqBuffersStats            = {preAllocateBuffers, 0, preAllocateBuffers, 0};
    respBuffersStats           = {preAllocateBuffers, 0, preAllocateBuffers, 0};
    for (int i = 0; i < preAllocateBuffers; i++) {
      reqBufferArray.push_back(allocate_req_buffer());
      respBufferArray.push_back(allocate_resp_buffer());
    }
    reqBufferMutex = NdbMutex_Create();
    respBufferMutex = NdbMutex_Create();
  }

  RS_BufferArrayManager(const RS_BufferArrayManager &)            = delete;
  RS_BufferArrayManager &operator=(const RS_BufferArrayManager &) = delete;
  RS_BufferArrayManager(RS_BufferArrayManager &&)                 = delete;
  RS_BufferArrayManager &operator=(RS_BufferArrayManager &&)      = delete;

  ~RS_BufferArrayManager() {
    NdbMutex_Lock(reqBufferMutex);
    NdbMutex_Lock(respBufferMutex);

    for (auto &buffer : reqBufferArray) {
      delete[] buffer.buffer;
    }
    for (auto &buffer : respBufferArray) {
      delete[] buffer.buffer;
    }
    NdbMutex_Unlock(reqBufferMutex);
    NdbMutex_Unlock(respBufferMutex);
    NdbMutex_Destroy(reqBufferMutex);
    NdbMutex_Destroy(respBufferMutex);
  }

  static RS_Buffer allocate_req_buffer() {
    auto reqBufferSize = globalConfigs.internal.reqBufferSize;
    auto buff = RS_Buffer();
    buff.buffer = new char[reqBufferSize];
    buff.size = reqBufferSize;
    return buff;
  }

  static RS_Buffer allocate_resp_buffer() {
    auto respBufferSize = globalConfigs.internal.respBufferSize;
    auto buff = RS_Buffer();
    buff.buffer = new char[respBufferSize];
    buff.size = respBufferSize;
    return buff;
  }

  RS_Buffer get_req_buffer() {
    NdbMutex_Lock(reqBufferMutex);
    auto numBuffersLeft = reqBufferArray.size();
    if (numBuffersLeft > 0) {
      auto buffer = reqBufferArray[numBuffersLeft - 1];
      reqBufferArray.pop_back();
      NdbMutex_Unlock(reqBufferMutex);
      return buffer;
    }
    auto buffer = allocate_req_buffer();
    reqBuffersStats.buffersCount++;
    reqBuffersStats.allocationsCount++;
    NdbMutex_Unlock(reqBufferMutex);
    return buffer;
  }

  RS_Buffer get_resp_buffer() {
    NdbMutex_Lock(respBufferMutex);
    auto numBuffersLeft = respBufferArray.size();
    if (numBuffersLeft > 0) {
      auto buffer = respBufferArray[numBuffersLeft - 1];
      respBufferArray.pop_back();
      NdbMutex_Unlock(respBufferMutex);
      return buffer;
    }
    auto buffer = allocate_resp_buffer();
    respBuffersStats.buffersCount++;
    respBuffersStats.allocationsCount++;
    NdbMutex_Unlock(respBufferMutex);
    return buffer;
  }

  void return_req_buffer(RS_Buffer buffer) {
    NdbMutex_Lock(reqBufferMutex);
    reqBufferArray.push_back(buffer);
    NdbMutex_Unlock(reqBufferMutex);
  }

  void return_resp_buffer(RS_Buffer buffer) {
    NdbMutex_Lock(respBufferMutex);
    respBufferArray.push_back(buffer);
    NdbMutex_Unlock(respBufferMutex);
  }

  MemoryStats get_req_buffers_stats() {
    // update the free buffers count
    NdbMutex_Lock(reqBufferMutex);
    reqBuffersStats.freeBuffers = static_cast<Int64>(reqBufferArray.size());
    MemoryStats ret = respBuffersStats;
    NdbMutex_Unlock(reqBufferMutex);
    return ret;
  }

  MemoryStats get_resp_buffers_stats() {
    // update the free buffers count
    NdbMutex_Lock(respBufferMutex);
    respBuffersStats.freeBuffers = static_cast<Int64>(respBufferArray.size());
    MemoryStats ret = respBuffersStats;
    NdbMutex_Unlock(respBufferMutex);
    return ret;
  }

 private:
  std::vector<RS_Buffer> reqBufferArray;
  std::vector<RS_Buffer> respBufferArray;
  MemoryStats reqBuffersStats{};
  MemoryStats respBuffersStats{};
  NdbMutex *reqBufferMutex;
  NdbMutex *respBufferMutex;
};

#define EN_STATUS_MSG_LEN 256
struct EN_Status {
  Uint32 retValue;
  HTTP_CODE http_code;                // 200 for successful operation
  char message[EN_STATUS_MSG_LEN]{};  // error message.
  EN_Status() : retValue(0), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  explicit EN_Status(HTTP_CODE http_code) : retValue(0),
                                            http_code(http_code), message("") {
  }
  explicit EN_Status(Uint32 retValue)
      : retValue(retValue), http_code(HTTP_CODE::SUCCESS), message("") {
  }
  EN_Status(HTTP_CODE http_code, const char *message) : retValue(0),
                                                        http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
  EN_Status(HTTP_CODE http_code, Uint32 retValue)
      : retValue(retValue), http_code(http_code), message("") {
  }
  EN_Status(HTTP_CODE http_code, Uint32 retValue, const char *message)
      : retValue(retValue), http_code(http_code) {
    strncpy(this->message, message, EN_STATUS_MSG_LEN - 1);
    this->message[EN_STATUS_MSG_LEN - 1] = '\0';
  }
};

extern RS_BufferArrayManager rsBufferArrayManager;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_BUFFER_MANAGER_HPP_
