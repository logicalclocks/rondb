/*
 * Copyright (c) 2024, 2026, Hopsworks and/or its affiliates.
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
#include "buffer_chain.hpp"
#include "api_key.hpp"
#include "config_structs.hpp"
#include "constants.hpp"
#include "rate_limit.hpp"
#include "metrics.hpp"
#include <NdbSleep.h>

#include <cstring>
#include <drogon/HttpTypes.h>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_PK_CTRL 1
#endif

#ifdef DEBUG_PK_CTRL
#define DEB_PK_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_PK_CTRL(...) do { } while (0)
#endif

void PKReadCtrl::pkRead(const drogon::HttpRequestPtr &req,
                        std::function<void(
                          const drogon::HttpResponsePtr &)> &&callback,
                        const std::string_view &db,
                        const std::string_view &table) {
  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  PkReadEndPointMetricsUpdater metricsUpdater(resp);
  bool use_compressed = globalConfigs.rest.useCompression;

  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    resp->setBody("Too many threads");
    DEB_PK_CTRL("Error message: Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
#ifdef DEBUG_PK_CTRL
  printf("\n\n JSON REQUEST: db: %s, tab: %s\n %s \n",
         db.data(), table.data(), json_str);
#endif
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.maxHttpReqSize)) {
    auto resp = drogon::HttpResponse::newHttpResponse();
    resp->setBody("Request too large");
    DEB_PK_CTRL("Error message: Request too large");
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
        globalConfigs.internal.maxHttpReqSize + simdjson::SIMDJSON_PADDING),
      reqStruct);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    DEB_PK_CTRL("Error message(1): %s", std::string(status.message).c_str());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate
  status = reqStruct.validate();
  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    DEB_PK_CTRL("Error message(2): %s", std::string(status.message).c_str());
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Authenticate
  std::string rl_identity;
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status = authenticate(api_key, reqStruct);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      DEB_PK_CTRL("Error message(3): %s", std::string(status.message).c_str());
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
    rl_identity = get_rate_limit_identity(api_key);
  }

  ArenaMalloc amalloc(64 * 1024);
  // Execute
  {
    /* RONDB-1144: one operation, one view in each chain; the chains return
     * the buffers when they go out of scope. */
    RS_BufferChain req_chain(RS_BufferChain::REQ, 1);
    RS_BufferChain resp_chain(RS_BufferChain::RESP, 1);
    status = req_chain.take_first();
    if (likely(status.http_code == SUCCESS)) {
      status = resp_chain.take_first();
    }
    if (unlikely(status.http_code != SUCCESS)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k503ServiceUnavailable);
      callback(resp);
      return;
    }
    RS_Buffer &reqBuff = req_chain.views()[0];
    Uint32 consumed = 0;
    status = create_native_request(reqStruct, reqBuff, consumed);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      DEB_PK_CTRL("Error message(4): %s", std::string(status.message).c_str());
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
    reqBuff.size = reinterpret_cast<Uint32 *>(reqBuff.buffer)[PK_REQ_LENGTH_IDX];
    req_chain.advance(consumed);

    // pk_read
    status = pk_batch_read((void*)&amalloc,
                           1,
                           false,
                           req_chain.views(),
                           &resp_chain,
                           currentThreadIndex,
                           rl_identity.empty() ? nullptr : rl_identity.c_str(),
                           (unsigned int)rl_identity.size());

    resp->setStatusCode(static_cast<drogon::HttpStatusCode>(status.http_code));
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      DEB_PK_CTRL("Error message(5): %s", std::string(status.message).c_str());
      resp->setBody(std::string(status.message));
    } else {
      if (use_compressed) {
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
      } else {
        resp->setContentTypeCode(drogon::CT_APPLICATION_OCTET_STREAM);
      }
      // convert resp to json
      PKReadResponseJSON respJson;
      size_t calc_size_json = 20; // Batch overhead
      process_pkread_response(&amalloc, resp_chain, 0, &reqBuff, respJson);
      calc_size_json += respJson.getSizeJson();
      char *json_buf = (char*)amalloc.alloc_bytes(calc_size_json, 8);
      if (likely(json_buf != nullptr)) {
        size_t size_json;
        respJson.to_string_single(json_buf, size_json);
#ifdef DEBUG_PK_CTRL
        printf("JSON response: len: %u, calc_len: %u: \n %s",
               (Uint32)size_json,
               (Uint32)calc_size_json,
               json_buf);
#endif
        std::string json(json_buf, size_json);
        resp->setBody(std::move(json));
      } else {
        amalloc.reset();
        resp->setBody("Malloc failure");
        resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
      }
    }
    callback(resp);
  }
}
