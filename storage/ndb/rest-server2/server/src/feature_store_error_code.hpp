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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_ERROR_CODE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_ERROR_CODE_HPP_

#include <string>
#include <map>
#include <sstream>
#include <simdjson.h>
#include <stdexcept>
#include <memory>

class RestErrorCode {
 public:
  RestErrorCode()                      = default;
  RestErrorCode(const RestErrorCode &) = default;
  explicit RestErrorCode(const std::shared_ptr<RestErrorCode> &code)
      : code_(code->GetCode()), reason_(code->GetReason()), status_(code->GetStatus()),
        message_(code->GetMessage()) {
  }
  RestErrorCode(const std::string &reason, int status)
      : code_(0), reason_(reason), status_(status) {
  }
  RestErrorCode(int code, const std::string &reason, int status, const std::string &message)
      : code_(code), reason_(reason), status_(status), message_(message) {
  }

  std::shared_ptr<RestErrorCode> NewMessage(const std::string &msg) const {
    return std::make_shared<RestErrorCode>(code_, reason_, status_, msg);
  }

  int GetCode() const {
    return code_;
  }
  std::string GetReason() const {
    return reason_;
  }
  int GetStatus() const {
    return status_;
  }
  std::string GetMessage() const {
    return message_;
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << "{"
        << "\"code\": " << code_ << ", "
        << "\"reason\": \"" << reason_ << "\", "
        << "\"message\": \"" << message_ << "\""
        << "}";
    return oss.str();
  }

  std::string Error() const {
    return ToString();
  }

  std::runtime_error GetError() const {
    return std::runtime_error(Error());
  }

 private:
  int code_{};
  std::string reason_;
  int status_{};
  std::string message_;
};

const std::shared_ptr<RestErrorCode> FV_NOT_EXIST =
    std::make_shared<RestErrorCode>(1, "Feature view does not exist.", 404, "");
const std::shared_ptr<RestErrorCode> FS_NOT_EXIST =
    std::make_shared<RestErrorCode>(2, "Feature store does not exist.", 404, "");
const std::shared_ptr<RestErrorCode> FG_NOT_EXIST =
    std::make_shared<RestErrorCode>(3, "Feature group does not exist.", 404, "");
const std::shared_ptr<RestErrorCode> FG_READ_FAIL =
    std::make_shared<RestErrorCode>(4, "Reading feature group failed.", 500, "");
const std::shared_ptr<RestErrorCode> FS_READ_FAIL =
    std::make_shared<RestErrorCode>(5, "Reading feature store failed.", 500, "");
const std::shared_ptr<RestErrorCode> FV_READ_FAIL =
    std::make_shared<RestErrorCode>(6, "Reading feature view failed.", 500, "");
const std::shared_ptr<RestErrorCode> TD_JOIN_READ_FAIL =
    std::make_shared<RestErrorCode>(7, "Reading training dataset join failed.", 500, "");
const std::shared_ptr<RestErrorCode> TD_FEATURE_READ_FAIL =
    std::make_shared<RestErrorCode>(8, "Reading training dataset feature failed.", 500, "");
const std::shared_ptr<RestErrorCode> FETCH_METADATA_FROM_CACHE_FAIL =
    std::make_shared<RestErrorCode>(9, "Fetching metadata from cache failed.", 500, "");
const std::shared_ptr<RestErrorCode> WRONG_DATA_TYPE =
    std::make_shared<RestErrorCode>(10, "Wrong data type.", 415, "");
const std::shared_ptr<RestErrorCode> FEATURE_NOT_EXIST =
    std::make_shared<RestErrorCode>(11, "Feature does not exist.", 404, "");
const std::shared_ptr<RestErrorCode> INCORRECT_PRIMARY_KEY =
    std::make_shared<RestErrorCode>(12, "Incorrect primary key.", 400, "");
const std::shared_ptr<RestErrorCode> INCORRECT_PASSED_FEATURE =
    std::make_shared<RestErrorCode>(13, "Incorrect passed feature.", 400, "");
const std::shared_ptr<RestErrorCode> READ_FROM_DB_FAIL =
    std::make_shared<RestErrorCode>(14, "Reading from db failed.", 500, "");
const std::shared_ptr<RestErrorCode> NO_PRIMARY_KEY_GIVEN =
    std::make_shared<RestErrorCode>(15, "No primary key is given.", 400, "");
const std::shared_ptr<RestErrorCode> INCORRECT_FEATURE_VALUE =
    std::make_shared<RestErrorCode>(16, "Incorrect feature value.", 400, "");
const std::shared_ptr<RestErrorCode> FEATURE_STORE_NOT_SHARED =
    std::make_shared<RestErrorCode>(17, "Accessing unshared feature store failed", 401, "");
const std::shared_ptr<RestErrorCode> READ_FROM_DB_FAIL_BAD_INPUT =
    std::make_shared<RestErrorCode>(18, "Reading from db failed.", 400, "");
const std::shared_ptr<RestErrorCode> DESERIALISE_FEATURE_FAIL =
    std::make_shared<RestErrorCode>(19, "Deserialising complex feature failed.", 500, "");

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_ERROR_CODE_HPP_