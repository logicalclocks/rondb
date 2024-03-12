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

#include "pk_read_ctrl.hpp"
#include "pk_data_structs.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "config_structs.hpp"
#include "constants.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>

void PKReadCtrl::pkRead(const drogon::HttpRequestPtr &req,
                        std::function<void(const drogon::HttpResponsePtr &)> &&callback,
                        const std::string &db, const std::string &table) {
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  if (length > REQ_BUFFER_SIZE) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }
  
  memcpy(jsonParser.get_buffer(currentThreadIndex).get(), json_str, length);

  PKReadParams reqStruct(db, table);

  RS_Status status = jsonParser.pk_parse(
      currentThreadIndex,
      simdjson::padded_string_view(jsonParser.get_buffer(currentThreadIndex).get(), length,
                                   REQ_BUFFER_SIZE + simdjson::SIMDJSON_PADDING),
      reqStruct);
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  status = reqStruct.validate();
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
    RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

    status = create_native_request(reqStruct, reqBuff.buffer, respBuff.buffer);
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
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
