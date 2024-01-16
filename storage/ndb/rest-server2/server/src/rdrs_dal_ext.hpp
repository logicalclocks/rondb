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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_EXT_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_EXT_HPP_

#include "rdrs_dal.h"

#include <cstdint>
#include <memory>

class RS_BufferManager {
 public:
  explicit RS_BufferManager(unsigned int size) : rsBuffer(std::make_unique<RS_Buffer>(size)) {
  }

  // Accessor method for underlying RS_Buffer
  RS_Buffer *getBuffer() const {
    return rsBuffer.get();
  }

 private:
  std::unique_ptr<RS_Buffer> rsBuffer;
};

class RS_BufferArrayManager {
 public:
  RS_BufferArrayManager(unsigned int numBuffers, unsigned int bufferSize)
      : rsBufferArray(new RS_Buffer[numBuffers]) {
    for (unsigned int i = 0; i < numBuffers; ++i) {
      rsBufferArray[i] = RS_Buffer(bufferSize);
    }
  }

  RS_Buffer *getBufferArray() const {
    return rsBufferArray.get();
  }

 private:
  std::unique_ptr<RS_Buffer[]> rsBufferArray;
  unsigned int numBuffers{};
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

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RDRS_DAL_EXT_HPP_
