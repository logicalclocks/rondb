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
#include "rdrs_dal_ext.hpp"
#include "pk_data_structs.hpp"
#include "src/constants.hpp"

#include <drogon/HttpTypes.h>
#include <iostream>
#include <simdjson.h>

void BatchPKReadCtrl::batchPKRead(const drogon::HttpRequestPtr &req,
                                  std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }
  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  size_t length        = req->getBody().length();
  strcpy(jsonParser.get_buffer(currentThreadIndex).get(), json_str);
  auto resp = drogon::HttpResponse::newHttpResponse();
  std::vector<PKReadParams> reqStructs;

  RS_Status status = jsonParser.batch_parse(
      currentThreadIndex,
      simdjson::padded_string_view(jsonParser.get_buffer(currentThreadIndex).get(), length,
                                   REQ_BUFFER_SIZE + simdjson::SIMDJSON_PADDING),
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

  for (auto reqStruct : reqStructs) {
    status = reqStruct.validate();
    if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    auto noOps = reqStructs.size();
    RS_BufferArrayManager reqBuffsManager =
        RS_BufferArrayManager(noOps, globalConfigs.internal.reqBufferSize);
    RS_BufferArrayManager respBuffsManager =
        RS_BufferArrayManager(noOps, globalConfigs.internal.respBufferSize);
    RS_Buffer *reqBuffs  = reqBuffsManager.getBufferArray();
    RS_Buffer *respBuffs = respBuffsManager.getBufferArray();

    for (unsigned long i = 0; i < noOps; i++) {
      status = create_native_request(reqStructs[i], reqBuffs[i].buffer, respBuffs[i].buffer);
      if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
      uintptr_t length_ptr = reinterpret_cast<uintptr_t>(reqBuffs[i].buffer) +
                             static_cast<uintptr_t>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      uint32_t *length_ptr_casted = reinterpret_cast<uint32_t *>(length_ptr);
      reqBuffs[i].size            = *length_ptr_casted;
    }

    // pk_batch_read
    status = pk_batch_read(noOps, reqBuffs, respBuffs);

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
  }
}
