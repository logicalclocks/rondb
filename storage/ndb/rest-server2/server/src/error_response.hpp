/*
 * Copyright (C) 2026 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_RESPONSE_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_RESPONSE_HPP_

#include "rdrs_dal.h"
#include "constants.hpp"

#include <drogon/drogon.h>
#include <string>

/**
 * Check if the request is using API version 0.2.0 or later (JSON errors)
 */
inline bool isJsonErrorVersion(const drogon::HttpRequestPtr &req) {
  const std::string &path = req->getPath();
  return path.find("/" API_VERSION_2 "/") != std::string::npos;
}

/**
 * Build a JSON error response body from RS_Status
 * Format:
 * {
 *   "error": {
 *     "code": 400,
 *     "message": "error message",
 *     "status": -1,
 *     "classification": -1,
 *     "ndbCode": -1,
 *     "mysqlCode": -1
 *   }
 * }
 */
inline std::string buildJsonErrorBody(const RS_Status &status) {
  std::string json = "{\"error\":{";
  json += "\"code\":" + std::to_string(status.http_code) + ",";
  json += "\"message\":\"";

  // Escape special characters in the message
  for (const char *p = status.message; *p != '\0'; ++p) {
    switch (*p) {
      case '"':  json += "\\\""; break;
      case '\\': json += "\\\\"; break;
      case '\n': json += "\\n"; break;
      case '\r': json += "\\r"; break;
      case '\t': json += "\\t"; break;
      default:   json += *p; break;
    }
  }

  json += "\",";
  json += "\"status\":" + std::to_string(status.status) + ",";
  json += "\"classification\":" + std::to_string(status.classification) + ",";
  json += "\"ndbCode\":" + std::to_string(status.code) + ",";
  json += "\"mysqlCode\":" + std::to_string(status.mysql_code);
  json += "}}";
  return json;
}

/**
 * Build a JSON error response body from a simple message
 */
inline std::string buildJsonErrorBody(HTTP_CODE http_code, const char *message) {
  RS_Status status;
  status.http_code = http_code;
  status.status = -1;
  status.classification = -1;
  status.code = -1;
  status.mysql_code = -1;
  strncpy(status.message, message, RS_STATUS_MSG_LEN - 1);
  status.message[RS_STATUS_MSG_LEN - 1] = '\0';
  return buildJsonErrorBody(status);
}

/**
 * Set error response on the HTTP response object
 * Uses JSON format for API version 0.2.0+, plain text for 0.1.0
 */
inline void setErrorResponse(const drogon::HttpRequestPtr &req,
                             drogon::HttpResponsePtr &resp,
                             const RS_Status &status) {
  resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));
  if (isJsonErrorVersion(req)) {
    resp->setBody(buildJsonErrorBody(status));
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  } else {
    resp->setBody(std::string(status.message));
  }
}

/**
 * Set error response with a simple message
 */
inline void setErrorResponse(const drogon::HttpRequestPtr &req,
                             drogon::HttpResponsePtr &resp,
                             HTTP_CODE http_code,
                             const char *message) {
  resp->setStatusCode(static_cast<drogon::HttpStatusCode>(http_code));
  if (isJsonErrorVersion(req)) {
    resp->setBody(buildJsonErrorBody(http_code, message));
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  } else {
    resp->setBody(message);
  }
}

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_ERROR_RESPONSE_HPP_
