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
#include "error_strings.h"
#include "rdrs_dal.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BPK_CTRL 1
#endif

#ifdef DEBUG_BPK_CTRL
#define DEB_BPK_CTRL(arglist) do { g_eventLogger->info arglist ; } while (0)
#else
#define DEB_BPK_CTRL(arglist) do { } while (0)
#endif


void BatchPKReadCtrl::batchPKRead(
  const drogon::HttpRequestPtr &req,
  std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp                 = drogon::HttpResponse::newHttpResponse();
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  DEB_BPK_CTRL(("\n\n JSON REQUEST: \n %s \n", json_str));
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.reqBufferSize)) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer().get(), json_str, length);

  std::vector<PKReadParams> reqStructs;

  RS_Status status = jsonParser.batch_parse(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.reqBufferSize +
                                   simdjson::SIMDJSON_PADDING),
      reqStructs);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (unlikely(reqStructs.size() > globalConfigs.internal.batchMaxSize)) {
    resp->setBody("Batch size exceeds maximum allowed size: " +
                  std::to_string(globalConfigs.internal.batchMaxSize));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate
  std::unordered_map<std::string_view, bool> db_map;
  std::unordered_map<std::string_view, bool> table_map;
  std::unordered_map<std::string_view, bool> column_map;
  std::vector<std::string_view> db_vector;

  for (auto reqStruct : reqStructs) {
    std::string_view db = reqStruct.path.db;
    std::string_view table = reqStruct.path.table;
    db_map[db] = true;
    table_map[table] = true;
    for (auto readColumn: reqStruct.readColumns) {
      std::string_view column = readColumn.column;
      column_map[column] = true;
    }
    for (auto filter: reqStruct.filters) {
      std::string_view column = filter.column;
      column_map[column] = true;
    }
  }
  for (auto it = db_map.begin(); it != db_map.end(); ++it) {
    auto db = it->first;
    status = validate_db_identifier(db);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    db_vector.push_back(db);
  }
  for (auto it = table_map.begin(); it != table_map.end(); ++it) {
    auto table = it->first;
    status = validate_db_identifier(table);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }
  for (auto it = column_map.begin(); it != column_map.end(); ++it) {
    auto column = it->first;
    status = validate_column(column);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }
  for (auto reqStruct : reqStructs) {
    if (reqStruct.filters.size() > 1) {
      status = reqStruct.validate_columns();
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
    }
    status = validate_operation_id(reqStruct.operationId);
    if (unlikely(status.http_code != static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k200OK))) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  // Authenticate
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status = authenticate(api_key, db_vector);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
  }

  // Execute
  {
    auto noOps = reqStructs.size();
    std::vector<RS_Buffer> reqBuffs(noOps);
    std::vector<RS_Buffer> respBuffs(noOps);

    for (unsigned long i = 0; i < noOps; i++) {
      RS_Buffer reqBuff  = rsBufferArrayManager.get_req_buffer();
      RS_Buffer respBuff = rsBufferArrayManager.get_resp_buffer();

      reqBuffs[i]  = reqBuff;
      respBuffs[i] = respBuff;

      status = create_native_request(reqStructs[i],
                                     reqBuff.buffer,
                                     respBuff.buffer);
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        return;
      }
      UintPtr length_ptr = reinterpret_cast<UintPtr>(reqBuff.buffer) +
        static_cast<UintPtr>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      Uint32 *length_ptr_casted = reinterpret_cast<Uint32*>(length_ptr);
      reqBuffs[i].size = *length_ptr_casted;
    }

    // pk_batch_read
    status = pk_batch_read(noOps, reqBuffs.data(), respBuffs.data());

    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));

    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
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
