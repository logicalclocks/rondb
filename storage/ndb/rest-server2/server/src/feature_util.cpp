/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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
#include "feature_util.hpp"
#include "prometheus_ctrl.hpp"
#include <drogon/HttpTypes.h>
#include <memory>
#include <optional>
#include <simdjson.h>
#include <vector>
#include <iostream>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_UTILS 1
#endif

#ifdef DEBUG_UTILS
#define DEB_UTILS(...)                                                         \
  do {                                                                         \
    g_eventLogger->info(__VA_ARGS__);                                          \
  } while (0)
#else
#define DEB_UTILS(...)                                                         \
  do {                                                                         \
  } while (0)
#endif

RS_Status
base64_decode(const std::string &encoded_string, std::string &decoded_string) {
  const char *src = encoded_string.c_str();
  size_t src_len  = encoded_string.size();
  std::vector<char> decoded_data(src_len);  // Allocate enough space

  const char *end_ptr = nullptr;
  int flags           = 0;  // No special flags
  Int64 decoded_len =
      base64_decode(src, src_len, decoded_data.data(), &end_ptr, flags);

  if (decoded_len < 0) {
    return CRS_Status(HTTP_CODE::SERVER_ERROR,
                      "Failed to decode base64 string.")
        .status;
  }

  decoded_string.assign(decoded_data.begin(),
                        decoded_data.begin() + decoded_len);
  return CRS_Status::SUCCESS.status;
}

std::tuple<std::shared_ptr<RestErrorCode>, std::vector<char>>
DeserialiseComplexFeature(std::vector<Uint8> &value,
                          const metadata::AvroDecoder &decoder) {
#ifdef DEBUG_UTILS
  std::vector<Uint8>::const_iterator it = value.begin();
  const Uint8* ptr = &(*it);
  DEB_UTILS("value: %p, len: %zu", ptr, value.size());
#endif
  auto [json_status, json] = decoder.decode(value);
  if (json_status.http_code != HTTP_CODE::SUCCESS) {
    DEB_UTILS("decode error: %u, message: %s",
      json_status.http_code,
      &json_status.message[0]);
    return std::make_tuple(
        std::make_shared<RestErrorCode>(json_status.message, json_status.http_code),
        std::vector<char>{});
  }
  return std::make_tuple(nullptr, json.value());
}

template <typename T>
void
AppendToVector(std::vector<char> &vec, const T &value) {
  const char *data = reinterpret_cast<const char *>(&value);
  vec.insert(vec.end(), data, data + sizeof(T));
}

void
AppendStringToVector(std::vector<char> &vec, const std::string &str) {
  vec.insert(vec.end(), str.begin(), str.end());
}

void
AppendBytesToVector(std::vector<char> &vec, const std::vector<Uint8> &bytes) {
  vec.insert(vec.end(), bytes.begin(), bytes.end());
}
