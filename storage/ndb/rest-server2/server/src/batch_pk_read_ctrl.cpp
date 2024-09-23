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

#include "batch_pk_read_ctrl.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "pk_data_structs.hpp"
#include "api_key.hpp"
#include "src/constants.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <simdjson.h>

void BatchPKReadCtrl::batchPKRead(const drogon::HttpRequestPtr &req,
                                  std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp                 = drogon::HttpResponse::newHttpResponse();
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }

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

  memcpy(jsonParser.get_buffer(currentThreadIndex).get(), json_str, length);

  std::vector<PKReadParams> reqStructs;

  RS_Status status = jsonParser.batch_parse(
      currentThreadIndex,
      simdjson::padded_string_view(jsonParser.get_buffer(currentThreadIndex).get(), length,
                                   globalConfigs.internal.reqBufferSize + simdjson::SIMDJSON_PADDING),
      reqStructs);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (reqStructs.size() > globalConfigs.internal.batchMaxSize) {
    resp->setBody("Batch size exceeds maximum allowed size: " +
                  std::to_string(globalConfigs.internal.batchMaxSize));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate
  for (auto reqStruct : reqStructs) {
    status = reqStruct.validate();
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  // Authenticate
  if (globalConfigs.security.apiKey.useHopsworksAPIKeys) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    for (auto reqStruct : reqStructs) {
      status = authenticate(api_key, reqStruct);
      if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k401Unauthorized);
        callback(resp);
        return;
      }
    }
  }

  // Execute
  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    auto noOps = reqStructs.size();
    std::vector<RS_Buffer> reqBuffs(noOps);
    std::vector<RS_Buffer> respBuffs(noOps);

    for (unsigned long i = 0; i < noOps; i++) {
      RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
      RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

      reqBuffs[i]  = reqBuff;
      respBuffs[i] = respBuff;

      status = create_native_request(reqStructs[i], reqBuff.buffer, respBuff.buffer);
      if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
      uintptr_t length_ptr = reinterpret_cast<uintptr_t>(reqBuff.buffer) +
                             static_cast<uintptr_t>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      uint32_t *length_ptr_casted = reinterpret_cast<uint32_t *>(length_ptr);
      reqBuffs[i].size            = *length_ptr_casted;
    }

    // pk_batch_read
    status = pk_batch_read(noOps, reqBuffs.data(), respBuffs.data());

    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));

    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
    } else {
      resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
      // convert resp to json
      std::vector<PKReadResponseJSON> responses;
      for (unsigned int i = 0; i < noOps; i++) {
        PKReadResponseJSON response;
        process_pkread_response(respBuffs[i].buffer, response);
        responses.push_back(response);
      }

      std::string json = PKReadResponseJSON::batch_to_string(responses);
      resp->setBody(json);
    }
    callback(resp);
    for (unsigned long i = 0; i < noOps; i++) {
      rsBufferArrayManager.return_resp_buffer(respBuffs[i]);
      rsBufferArrayManager.return_req_buffer(reqBuffs[i]);
    }
  }
}
