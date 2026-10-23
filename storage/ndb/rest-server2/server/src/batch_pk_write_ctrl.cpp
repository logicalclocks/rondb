/*
 * Copyright (C) 2023, 2026 Hopsworks AB
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

#include "batch_pk_write_ctrl.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "buffer_chain.hpp"
#include "pk_data_structs.hpp"
#include "api_key.hpp"
#include "src/constants.hpp"
#include "rate_limit.hpp"
#include "metrics.hpp"
#include "error_response.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <memory>
#include <new>
#include <set>
#include <simdjson.h>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>
#include <util/require.h>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_BPK_WRITE_CTRL 1
#endif

#ifdef DEBUG_BPK_WRITE_CTRL
#define DEB_BPK_WRITE_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_BPK_WRITE_CTRL(...) do { } while (0)
#endif


void BatchPKWriteCtrl::batchPKWrite(
  const drogon::HttpRequestPtr &req,
  std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  BatchPkWriteEndPointMetricsUpdater metricsUpdater(resp);
  bool use_compressed = globalConfigs.rest.useCompression;

  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    setErrorResponse(req, resp, SERVER_ERROR, "Too many threads");
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
#ifdef DEBUG_BPK_WRITE_CTRL
  printf("\n\n JSON REQUEST: \n %s \n", json_str);
#endif
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.maxHttpReqSize)) {
    setErrorResponse(req, resp, CLIENT_ERROR, "Request too large");
    callback(resp);
    return;
  }

  memcpy(jsonParser.get_buffer().get(), json_str, length);

  std::vector<PKReadParams> reqStructs;

  RS_Status status = jsonParser.batch_parse_write(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.maxHttpReqSize +
                                   simdjson::SIMDJSON_PADDING),
      reqStructs);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    setErrorResponse(req, resp, status);
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
    // Validate writeColumns
    for (auto writeColumn: reqStruct.writeColumns) {
      std::string_view column = writeColumn.column;
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
      setErrorResponse(req, resp, status);
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
      setErrorResponse(req, resp, status);
      callback(resp);
      return;
    }
  }
  for (auto it = column_map.begin(); it != column_map.end(); ++it) {
    auto column = it->first;
    status = validate_column(column);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      setErrorResponse(req, resp, status);
      callback(resp);
      return;
    }
  }
  for (auto reqStruct : reqStructs) {
    if (reqStruct.filters.size() > 1) {
      status = reqStruct.validate_columns();
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
        setErrorResponse(req, resp, status);
        callback(resp);
        return;
      }
    }
    status = validate_operation_id(reqStruct.operationId);
    if (unlikely(status.http_code != static_cast<HTTP_CODE>(
          drogon::HttpStatusCode::k200OK))) {
      setErrorResponse(req, resp, status);
      callback(resp);
      return;
    }
  }

  // Authenticate
  std::string rl_identity;
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    // One access request per (db, table) of the batch. Modifying data
    // requires access to the whole table, so no per-column check applies.
    std::set<std::pair<std::string_view, std::string_view>> table_accesses;
    for (auto &reqStruct : reqStructs) {
      table_accesses.insert({reqStruct.path.db, reqStruct.path.table});
    }
    std::vector<TableAccessRequest> accessReqs;
    accessReqs.reserve(table_accesses.size());
    for (auto &entry : table_accesses) {
      TableAccessRequest accessReq;
      accessReq.db = entry.first;
      accessReq.table = entry.second;
      accessReq.columns = nullptr;
      accessReqs.push_back(accessReq);
    }
    status = authenticate(api_key, accessReqs);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      setErrorResponse(req, resp, status);
      callback(resp);
      return;
    }
    rl_identity = get_rate_limit_identity(api_key);
  }
  ArenaMalloc amalloc(256 * 1024);
  // Execute
  {
    auto noOps = reqStructs.size();
    /* RONDB-1144: every buffer this request takes lives in these chains
     * and is returned when they go out of scope. */
    RS_BufferChain req_chain(RS_BufferChain::REQ, noOps);
    RS_BufferChain resp_chain(RS_BufferChain::RESP, noOps);
    status = req_chain.take_first();
    if (likely(status.http_code == SUCCESS)) {
      status = resp_chain.take_first();
    }
    if (unlikely(status.http_code != SUCCESS)) {
      setErrorResponse(req, resp, status);  // 503: pool exhausted
      callback(resp);
      return;
    }
    for (Uint32 i = 0; i < noOps; i++) {
      if (i > 0) {
        status = req_chain.next_op_view(i, native_write_request_upper_bound(reqStructs[i]));
        if (unlikely(status.http_code != SUCCESS)) {
          setErrorResponse(req, resp, status);  // 503: pool exhausted
          callback(resp);
          return;
        }
      }
      RS_Buffer &view = req_chain.views()[i];
      DEB_BPK_WRITE_CTRL("Request view %u: %p, size: %u", i, view.buffer, view.size);
      Uint32 consumed = 0;
      status = create_native_write_request(reqStructs[i], view, consumed);
      if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
        setErrorResponse(req, resp, status);
        callback(resp);
        return;
      }
      /* The view's size becomes the operation request's length, as the
       * NDB layer expects; the chain keeps the real buffer size. */
      view.size = reinterpret_cast<Uint32 *>(view.buffer)[PK_REQ_LENGTH_IDX];
      req_chain.advance(consumed);
    }

    metricsUpdater.set_key_requests(noOps);

    // pk_batch_write
    status = pk_batch_write(&amalloc,
                            noOps,
                            true,
                            req_chain.views(),
                            &resp_chain,
                            currentThreadIndex,
                            rl_identity.empty() ? nullptr : rl_identity.c_str(),
                             (unsigned int)rl_identity.size());

    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      setErrorResponse(req, resp, status);
    } else {
      resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));
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
        RS_Status read_status =
          process_pkread_response(&amalloc,
                                  resp_chain,
                                  i,
                                  &req_chain.views()[i],
                                  response);
        /* An internal error (it names a file) leaves the columns unset, so
         * nothing can be serialised; an operation status such as 404 can. */
        if (unlikely(read_status.err_file_name[0] != '\0')) {
          setErrorResponse(req, resp, read_status);
          callback(resp);
          return;
        }
        calc_size_json += response.getSizeJson();
        responses.push_back(response);
      }
      /* RONDB-1144: the JSON is written straight into the response body,
       * so one copy of it exists next to the pool buffers it is read from;
       * the buffer pool's cap counts on that (see derive_max_buffers). */
      std::string json;
      try {
        json.resize(calc_size_json);
      } catch (const std::bad_alloc &) {
        resp->setBody("Malloc failure");
        resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
        callback(resp);
        return;
      }
      size_t size_json;
      PKReadResponseJSON::batch_to_string(responses, json.data(), size_json);
      json.resize(size_json);
#ifdef DEBUG_BPK_WRITE_CTRL
      printf("Response string: len: %u, calc_len: %u: %s",
        (Uint32)size_json,
        (Uint32)calc_size_json,
        json.c_str());
#endif
      resp->setBody(std::move(json));
    }
    callback(resp);
  }
}
