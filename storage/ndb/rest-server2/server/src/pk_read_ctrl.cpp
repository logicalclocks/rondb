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

#include "pk_read_ctrl.hpp"
#include "pk_data_structs.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "api_key.hpp"
#include "config_structs.hpp"
#include "constants.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>

void PKReadCtrl::pkRead(const drogon::HttpRequestPtr &req,
                        std::function<void(const drogon::HttpResponsePtr &)> &&callback,
                        const std::string &db, const std::string &table) {
  auto resp                 = drogon::HttpResponse::newHttpResponse();
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  if (length > globalConfigs.internal.reqBufferSize) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer().get(), json_str, length);

  PKReadParams reqStruct(db, table);

  RS_Status status = jsonParser.pk_parse(
      simdjson::padded_string_view(
        jsonParser.get_buffer().get(),
        length,
        globalConfigs.internal.reqBufferSize + simdjson::SIMDJSON_PADDING),
      reqStruct);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate
  status = reqStruct.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Authenticate
  if (globalConfigs.security.apiKey.useHopsworksAPIKeys) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status       = authenticate(api_key, reqStruct);
    if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.code);
      callback(resp);
      return;
    }
  }

  // Execute
  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
    RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

    status = create_native_request(reqStruct, reqBuff.buffer, respBuff.buffer);
    if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    uintptr_t length_ptr = reinterpret_cast<uintptr_t>(reqBuff.buffer) +
                           static_cast<uintptr_t>(PK_REQ_LENGTH_IDX * ADDRESS_SIZE);
    uint32_t *length_ptr_casted = reinterpret_cast<uint32_t *>(length_ptr);
    reqBuff.size                = *length_ptr_casted;

    // pk_read
    status = pk_read(&reqBuff, &respBuff);

    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK)
      resp->setBody(std::string(status.message));
    else {
      resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
      // convert resp to json
      char *respData = respBuff.buffer;

      PKReadResponseJSON respJson;
      respJson.init();
      process_pkread_response(respData, respJson);

      resp->setBody(respJson.to_string());
    }
    callback(resp);
    rsBufferArrayManager.return_resp_buffer(respBuff);
    rsBufferArrayManager.return_req_buffer(reqBuff);
  }
}
