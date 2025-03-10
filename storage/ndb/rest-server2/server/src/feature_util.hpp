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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_UTIL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_UTIL_HPP_

#include "feature_store_error_code.hpp"
#include "metadata.hpp"
#include <optional>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <any>
#include <stdexcept>
#include <simdjson.h>
#include "base64.h"

RS_Status
base64_decode(const std::string &encoded_string, std::string &decoded_string);

std::tuple<std::shared_ptr<RestErrorCode>, std::vector<char>>
DeserialiseComplexFeature(std::vector<Uint8> &value,
                          const metadata::AvroDecoder &decoder);
#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_UTIL_HPP_
