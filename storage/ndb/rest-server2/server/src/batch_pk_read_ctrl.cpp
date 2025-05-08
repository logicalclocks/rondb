/*
 * Copyright (C) 2023, 2025 Hopsworks AB
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
#include "metrics.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>
#include <util/require.h>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BPK_CTRL 1
#endif

#ifdef DEBUG_BPK_CTRL
#define DEB_BPK_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_BPK_CTRL(...) do { } while (0)
#endif


void BatchPKReadCtrl::batchPKRead(
  const drogon::HttpRequestPtr &req,
  std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  BatchPkReadEndPointMetricsUpdater metricsUpdater(resp);
  bool use_compressed = globalConfigs.rest.useCompression;

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
#ifdef DEBUG_BPK_CTRL
  printf("\n\n JSON REQUEST: \n %s \n", json_str);
#endif
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.maxReqSize)) {
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
                                   globalConfigs.internal.maxReqSize +
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
  std::unordered_map<std::string, bool> table_map;
  std::unordered_map<std::string_view, bool> column_map;
  std::vector<std::string_view> db_vector;

  for (auto reqStruct : reqStructs) {
    std::string_view &db = reqStruct.path.db;
    std::string &table = reqStruct.path.table;
    db_map[db] = true;
    table_map[table] = true;
    for (auto readColumn: reqStruct.readColumns) {
      std::string_view &column = readColumn.column;
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
    const std::string &table = it->first;
    const std::string_view table_view = table;
    status = validate_db_identifier(table_view);
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
  ArenaMalloc amalloc(256 * 1024);
  // Execute
  {
    auto noOps = reqStructs.size();
    std::vector<RS_Buffer> reqBuffs(noOps);
    std::vector<RS_Buffer> respBuffs(noOps);
    Uint32 request_buffer_size = globalConfigs.internal.reqBufferSize * 2;
    Uint32 request_buffer_limit = request_buffer_size / 2;
    Uint32 current_head = 0;
    reqBuffs[0] = rsBufferArrayManager.get_req_buffer();
    respBuffs[0] = rsBufferArrayManager.get_resp_buffer();
    Uint32 current_request_buffer_idx = 0;
    for (Uint32 i = 0; i < noOps; i++) {
      if (i > 0)
        reqBuffs[i] = getNextReqRS_Buffer(
          current_head,
          request_buffer_limit,
          reqBuffs[current_request_buffer_idx],
          current_request_buffer_idx,
          i
        );
      DEB_BPK_CTRL("Buffer: %p, current_head: %u",
        reqBuff.buffer, current_head);
      status = create_native_request(reqStructs[i],
                                     (Uint32*)reqBuffs[i].buffer,
                                     current_head);
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
        resp->setBody(std::string(status.message));
        resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
        callback(resp);
        release_array_buffers(reqBuffs.data(), respBuffs.data(), i);
        return;
      }
      UintPtr length_ptr = reinterpret_cast<UintPtr>(reqBuffs[i].buffer) +
        static_cast<UintPtr>(PK_REQ_LENGTH_IDX) * ADDRESS_SIZE;
      Uint32 *length_ptr_casted = reinterpret_cast<Uint32*>(length_ptr);
      reqBuffs[i].size = *length_ptr_casted;
      require(reqBuffs[i].buffer + reqBuffs[i].size <=
              reqBuffs[current_request_buffer_idx].buffer + current_head);
    }

    metricsUpdater.set_key_requests(noOps);

    // pk_batch_read
    status = pk_batch_read(&amalloc,
                           noOps,
                           true,
                           reqBuffs.data(),
                           respBuffs.data(),
                           currentThreadIndex);

    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));

    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
    } else {
      if (use_compressed) {
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
      } else {
        resp->setContentTypeCode(drogon::CT_APPLICATION_OCTET_STREAM);
      }
      // convert resp to json
      std::vector<PKReadResponseJSON> responses;
      size_t calc_size_json = 20; // Batch overhead
      for (unsigned int i = 0; i < noOps; i++) {
        PKReadResponseJSON response;
        DEB_BPK_CTRL("Response buffer[%u]: %p, size: %u",
          i, respBuffs[i].buffer, respBuffs[i].size);
        process_pkread_response(&amalloc,
                                respBuffs[i].buffer,
                                &reqBuffs[i],
                                response);
        calc_size_json += response.getSizeJson();
        responses.push_back(response);
      }
      char *json_buf = (char*)amalloc.alloc_bytes(calc_size_json, 8);
      if (likely(json_buf != nullptr)) {
        size_t size_json;
        int ret = PKReadResponseJSON::batch_to_string(responses,
                                                      json_buf,
                                                      &amalloc,
                                                      size_json);
        if (unlikely(ret < 0)) {
          amalloc.reset();
          resp->setBody("Malloc failure");
          resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
        } else {
#ifdef DEBUG_BPK_CTRL
          printf("Response string: len: %u, calc_len: %u: %s",
            (Uint32)size_json,
            (Uint32)calc_size_json,
            json_buf);
#endif
          std::string json(json_buf, size_json);
          resp->setBody(std::move(json));
        }
      } else {
        amalloc.reset();
        resp->setBody("Malloc failure");
        resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
      }
    }
    callback(resp);
    release_array_buffers(reqBuffs.data(), respBuffs.data(), noOps);
  }
}
