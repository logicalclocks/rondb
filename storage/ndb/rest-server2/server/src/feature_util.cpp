/*
 * Copyright (C) 2024 Hopsworks AB
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
#include "avro/Stream.hh"
#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>
#include <vector>

std::string base64_decode(const std::string &encoded_string) {
  const char *src = encoded_string.c_str();
  size_t src_len  = encoded_string.size();
  std::vector<char> decoded_data(src_len);  // Allocate enough space

  const char *end_ptr = nullptr;
  int flags           = 0;  // No special flags
  Int64 decoded_len = base64_decode(src,
                                    src_len,
                                    decoded_data.data(),
                                    &end_ptr,
                                    flags);

  if (decoded_len < 0) {
    throw std::runtime_error("Failed to decode base64 string.");
  }

  return std::string(decoded_data.begin(), decoded_data.begin() + decoded_len);
}

std::tuple<std::vector<char>, std::shared_ptr<RestErrorCode>>
DeserialiseComplexFeature(
  const std::vector<char> &value, const metadata::AvroDecoder &decoder) {
  std::string valueString(value.begin(), value.end());
  simdjson::dom::parser parser;
  simdjson::dom::element element;

  auto error = parser.parse(valueString).get(element);
  if (error != 0U) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
          "Failed to unmarshal JSON value.",
          static_cast<int>(drogon::k500InternalServerError))
    );
  }

  valueString = element.get_string().value();
  std::string jsonDecode;
  try {
    jsonDecode = base64_decode(valueString);
  } catch (const std::runtime_error &e) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
          "Failed to decode base64 value.",
          static_cast<int>(drogon::k400BadRequest))
    );
  }

  std::vector<Uint8> binaryData(jsonDecode.begin(), jsonDecode.end());
  avro::GenericDatum native;
  try {
    native = decoder.decode(binaryData);
  } catch (const std::runtime_error &e) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
          e.what(),
          static_cast<int>(drogon::k400BadRequest))
    );
  }

  auto nativeJson = ConvertAvroToJson(native, decoder.getSchema());
  if (std::get<1>(nativeJson).code != HTTP_CODE::SUCCESS) {
    return std::make_tuple(
        std::vector<char>{},
        std::make_shared<RestErrorCode>(
          "Failed to convert Avro to JSON.",
          static_cast<int>(drogon::k500InternalServerError))
    );
  }
  return std::make_tuple(std::get<0>(nativeJson), nullptr);
}

template <typename T> void AppendToVector(std::vector<char> &vec,
                                          const T &value) {
  const char *data = reinterpret_cast<const char *>(&value);
  vec.insert(vec.end(), data, data + sizeof(T));
}

void AppendStringToVector(std::vector<char> &vec, const std::string &str) {
  vec.insert(vec.end(), str.begin(), str.end());
}

void AppendBytesToVector(std::vector<char> &vec,
                         const std::vector<Uint8> &bytes) {
  vec.insert(vec.end(), bytes.begin(), bytes.end());
}

std::tuple<std::vector<char>, RS_Status>
ConvertAvroToJson(const avro::GenericDatum &datum,
                  const avro::ValidSchema &schema) {
  std::vector<char> result;

  std::ostringstream oss;
  std::unique_ptr<avro::OutputStream> out = avro::ostreamOutputStream(oss);
  avro::EncoderPtr jsonEncoder = avro::jsonEncoder(schema);
  jsonEncoder->init(*out);
  avro::encode(*jsonEncoder, datum);
  jsonEncoder->flush();

  std::string jsonStr = oss.str();

  simdjson::ondemand::parser parser;
  simdjson::padded_string padded_json = simdjson::padded_string(jsonStr);
  simdjson::ondemand::document doc;
  auto error = parser.iterate(padded_json).get(doc);
  if (error != 0U) {
    return std::make_tuple(
        std::vector<char>{},
        CRS_Status(HTTP_CODE::SERVER_ERROR, "Failed to parse JSON").status
    );
  }

  return {{jsonStr.begin(), jsonStr.end()}, CRS_Status::SUCCESS.status};
}
