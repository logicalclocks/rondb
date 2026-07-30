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

#include "scan_read_ctrl.hpp"
#include "json_parser.hpp"
#include "encoding.hpp"
#include "buffer_manager.hpp"
#include "pk_data_structs.hpp"
#include "api_key.hpp"
#include "src/constants.hpp"
#include "rate_limit.hpp"
#include "metrics.hpp"
#include "scan_metrics.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <iostream>
#include <functional>
#include <memory>
#include <simdjson.h>
#include <EventLogger.hpp>
#include <ArenaMalloc.hpp>
#include <util/require.h>

extern EventLogger *g_eventLogger;

#include <rapidjson/document.h>      // rapidjson::Document
#include <rapidjson/stringbuffer.h>

typedef rapidjson::UTF8<char> RJ_Encoding;
typedef rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator> RJ_Allocator;
typedef rapidjson::GenericDocument<RJ_Encoding, RJ_Allocator,
                                   rapidjson::CrtAllocator> RJ_Document;
typedef rapidjson::GenericStringBuffer<RJ_Encoding,
                                       rapidjson::CrtAllocator> RJ_StringBuffer;

void ScanReadCtrl::ScanRead(
       const drogon::HttpRequestPtr& req,
       std::function<void(const drogon::HttpResponsePtr &)>&& callback,
       const std::string_view& db,
       const std::string_view& table) {

  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  IndexScanEndPointMetricsUpdater metricsUpdater(resp);

  // Timing setup
  bool timing_enabled = g_scan_timing_enabled;
  ScanPhaseTiming timing;
  NDB_TICKS total_start, phase_start;
  if (timing_enabled) {
    total_start = NdbTick_getCurrentTicks();
    phase_start = total_start;
  }

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
#ifdef DEBUG_SCAN_CTRL
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

  ScanReadParams reqStruct(db, table);

  RS_Status status = jsonParser.scan_parse(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.maxReqSize +
                                   simdjson::SIMDJSON_PADDING),
                                   reqStruct);

  // End of json_parse phase
  if (timing_enabled) {
    timing.json_parse_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    phase_start = NdbTick_getCurrentTicks();
  }

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validation
  status = validate_db(reqStruct.path.db);
  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  const std::string_view table_view = reqStruct.path.table;
  status = validate_table(table_view);
  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (!reqStruct.readColumns.empty()) {
    status = ValidateScanColumns(reqStruct.readColumns);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  if (reqStruct.filterRoot) {
    status = ValidateScanFilter(reqStruct.filterRoot);
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  if (reqStruct.index != std::nullopt) {
    status = ValidateScanIndex(reqStruct.index.value());
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
      callback(resp);
      return;
    }
  }

  // Authenticate
  std::string rl_identity;
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    // A scan with no explicit read columns returns whole rows, so the
    // whole table must be granted. Filter and index-key columns expose
    // data too (their values are compared), so they are checked as well.
    std::vector<std::string_view> columns;
    for (const ScanReadColumn &readColumn : reqStruct.readColumns) {
      columns.push_back(readColumn.column);
    }
    if (!reqStruct.readColumns.empty()) {
      std::function<void(const std::shared_ptr<FilterNode>&)> collect =
        [&](const std::shared_ptr<FilterNode> &node) {
          if (node == nullptr) {
            return;
          }
          if (!node->column.empty()) {
            columns.push_back(node->column);
          }
          for (const std::shared_ptr<FilterNode> &child : node->children) {
            collect(child);
          }
        };
      collect(reqStruct.filterRoot);
      if (reqStruct.index != std::nullopt) {
        for (const std::string &column : reqStruct.index->columns) {
          columns.push_back(column);
        }
      }
    }
    TableAccessRequest accessReq;
    accessReq.db = reqStruct.path.db;
    accessReq.table = reqStruct.path.table;
    accessReq.columns = reqStruct.readColumns.empty() ? nullptr : &columns;
    status = authenticate(api_key,
                          std::vector<TableAccessRequest>{accessReq});
    if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
        drogon::HttpStatusCode::k200OK)) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
    rl_identity = get_rate_limit_identity(api_key);
  }

  RJ_Document doc;
  RJ_StringBuffer buf;
  buf.Reserve(globalConfigs.internal.scanRespBufferSize);

  // End of validation phase (includes auth and buffer reserve)
  if (timing_enabled) {
    timing.validation_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
  }

  uint64_t rows_fetched = 0;
  status = scan_read(reqStruct, currentThreadIndex, (void*)&buf,
                     rl_identity.empty() ? nullptr : rl_identity.c_str(),
                     (unsigned int)rl_identity.size(),
                     &rows_fetched, timing_enabled ? &timing : nullptr);

  if (unlikely(static_cast<drogon::HttpStatusCode>(status.http_code) !=
      drogon::HttpStatusCode::k200OK)) {
    resp->setBody(std::string(status.message));
    /* Rate limit rejections (RONDB-978) must surface as 429, everything
       else keeps the historical 400 of this endpoint */
    resp->setStatusCode(status.http_code == TOO_MANY_REQUESTS
                          ? drogon::HttpStatusCode::k429TooManyRequests
                          : drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  metricsUpdater.set_rows_fetched(rows_fetched);

  // Start callback timing
  if (timing_enabled) {
    phase_start = NdbTick_getCurrentTicks();
  }

  resp->setBody(std::string(buf.GetString(), buf.GetSize()));
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);

  // End of callback phase and record metrics
  if (timing_enabled) {
    timing.callback_us = NdbTick_Elapsed(phase_start, NdbTick_getCurrentTicks()).microSec();
    timing.total_us = NdbTick_Elapsed(total_start, NdbTick_getCurrentTicks()).microSec();

    // Fill in context
    timing.database = std::string(reqStruct.path.db);
    timing.table = std::string(reqStruct.path.table);
    timing.limit = reqStruct.limit;
    timing.has_filter = (reqStruct.filterRoot != nullptr);
    timing.is_index_scan = (reqStruct.index != std::nullopt);
    if (timing.is_index_scan) {
      timing.index_name = reqStruct.index.value().name;
    }

    maybeRecordSlowScan(timing, currentThreadIndex);
  }
  return;
}
