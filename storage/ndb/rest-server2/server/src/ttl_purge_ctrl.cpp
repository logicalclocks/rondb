/*
 * Copyright (C) 2025 Hopsworks and/or its affiliates
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

#include "ttl_purge_ctrl.hpp"
#include "config_structs.hpp"
#include "api_key.hpp"

#include <cstring>
#include <drogon/HttpTypes.h>
#include <simdjson.h>
#include <EventLogger.hpp>

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

extern EventLogger *g_eventLogger;

// Helper to check if TTL purger is available
static bool checkPurgerAvailable(
    drogon::HttpResponsePtr &resp,
    std::function<void(const drogon::HttpResponsePtr &)> &callback) {
  if (g_ttl_purger == nullptr) {
    resp->setBody("{\"error\": \"TTL purger not initialized\"}");
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setStatusCode(drogon::HttpStatusCode::k503ServiceUnavailable);
    callback(resp);
    return false;
  }
  return true;
}

const char* TTLPurgeCtrl::stateToString(TTLPurgeStatus::State state) {
  switch (state) {
    case TTLPurgeStatus::State::kStopped:
      return "stopped";
    case TTLPurgeStatus::State::kRunning:
      return "running";
    case TTLPurgeStatus::State::kPaused:
      return "paused";
    case TTLPurgeStatus::State::kDisabled:
      return "disabled";
    case TTLPurgeStatus::State::kError:
      return "error";
    default:
      return "unknown";
  }
}

std::string TTLPurgeCtrl::configToJson(const TTLPurgeConfig &config) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  doc.AddMember("enabled", config.enabled, allocator);
  doc.AddMember("min_batch_size", config.min_batch_size, allocator);
  doc.AddMember("max_batch_size", config.max_batch_size, allocator);
  doc.AddMember("sleep_interval_ms", config.sleep_interval_ms, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string TTLPurgeCtrl::statusToJson(const TTLPurgeStatus &status) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  doc.AddMember("state",
      rapidjson::Value(stateToString(status.state), allocator), allocator);
  doc.AddMember("schema_watcher_running", status.schema_watcher_running,
                allocator);
  doc.AddMember("purge_worker_running", status.purge_worker_running, allocator);
  doc.AddMember("current_table",
      rapidjson::Value(status.current_table.c_str(), allocator), allocator);
  doc.AddMember("current_partition", status.current_partition, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string TTLPurgeCtrl::metricsToJson(const TTLPurgeMetrics &metrics) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  doc.AddMember("tables_count", metrics.tables_count, allocator);
  doc.AddMember("rows_purged_total", (uint64_t)metrics.rows_purged_total,
                allocator);
  doc.AddMember("rows_purged_last_round",
                (uint64_t)metrics.rows_purged_last_round, allocator);
  doc.AddMember("last_round_duration_ms",
                (uint64_t)metrics.last_round_duration_ms, allocator);
  doc.AddMember("last_purge_time_epoch_ms",
                (uint64_t)metrics.last_purge_time_epoch_ms, allocator);
  doc.AddMember("rounds_completed", (uint64_t)metrics.rounds_completed,
                allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

std::string TTLPurgeCtrl::tableMetricsToJson(const TTLTableMetrics &table) {
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  doc.AddMember("database",
      rapidjson::Value(table.database.c_str(), allocator), allocator);
  doc.AddMember("table",
      rapidjson::Value(table.table.c_str(), allocator), allocator);
  doc.AddMember("table_id", table.table_id, allocator);
  doc.AddMember("ttl_sec", table.ttl_sec, allocator);
  doc.AddMember("ttl_column_no", table.ttl_column_no, allocator);
  doc.AddMember("current_partition", table.current_partition, allocator);
  doc.AddMember("partition_count", table.partition_count, allocator);
  doc.AddMember("current_batch_size", table.current_batch_size, allocator);
  doc.AddMember("rows_purged", (uint64_t)table.rows_purged, allocator);
  doc.AddMember("last_purge_time_epoch_ms",
                (uint64_t)table.last_purge_time_epoch_ms, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);
  return buffer.GetString();
}

void TTLPurgeCtrl::getAll(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  TTLPurgeConfig config = g_ttl_purger->GetConfig();
  TTLPurgeStatus status = g_ttl_purger->GetStatus();
  TTLPurgeMetrics metrics = g_ttl_purger->GetMetrics();

  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  // Config object
  rapidjson::Value configObj(rapidjson::kObjectType);
  configObj.AddMember("enabled", config.enabled, allocator);
  configObj.AddMember("min_batch_size", config.min_batch_size, allocator);
  configObj.AddMember("max_batch_size", config.max_batch_size, allocator);
  configObj.AddMember("sleep_interval_ms", config.sleep_interval_ms, allocator);
  doc.AddMember("config", configObj, allocator);

  // Status object
  rapidjson::Value statusObj(rapidjson::kObjectType);
  statusObj.AddMember("state",
      rapidjson::Value(stateToString(status.state), allocator), allocator);
  statusObj.AddMember("schema_watcher_running", status.schema_watcher_running,
                      allocator);
  statusObj.AddMember("purge_worker_running", status.purge_worker_running,
                      allocator);
  statusObj.AddMember("current_table",
      rapidjson::Value(status.current_table.c_str(), allocator), allocator);
  statusObj.AddMember("current_partition", status.current_partition, allocator);
  doc.AddMember("status", statusObj, allocator);

  // Metrics object
  rapidjson::Value metricsObj(rapidjson::kObjectType);
  metricsObj.AddMember("tables_count", metrics.tables_count, allocator);
  metricsObj.AddMember("rows_purged_total", (uint64_t)metrics.rows_purged_total,
                       allocator);
  metricsObj.AddMember("rows_purged_last_round",
                       (uint64_t)metrics.rows_purged_last_round, allocator);
  metricsObj.AddMember("last_round_duration_ms",
                       (uint64_t)metrics.last_round_duration_ms, allocator);
  metricsObj.AddMember("last_purge_time_epoch_ms",
                       (uint64_t)metrics.last_purge_time_epoch_ms, allocator);
  metricsObj.AddMember("rounds_completed", (uint64_t)metrics.rounds_completed,
                       allocator);
  doc.AddMember("metrics", metricsObj, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  resp->setBody(buffer.GetString());
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::getConfig(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  TTLPurgeConfig config = g_ttl_purger->GetConfig();
  resp->setBody(configToJson(config));
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::updateConfig(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  // Parse JSON body
  const std::string &body = std::string(req->getBody());
  if (body.empty()) {
    resp->setBody("{\"error\": \"Request body is empty\"}");
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Get current config and update with provided values
  TTLPurgeConfig config = g_ttl_purger->GetConfig();

  // Parse using simdjson
  simdjson::ondemand::parser parser;
  simdjson::padded_string padded_body(body);
  auto doc = parser.iterate(padded_body);
  if (doc.error()) {
    resp->setBody("{\"error\": \"Invalid JSON\"}");
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Try to get each field
  try {
    auto obj = doc.get_object();
    for (auto field : obj) {
      std::string_view key = field.unescaped_key();
      if (key == "enabled") {
        auto val = field.value().get_bool();
        if (!val.error()) {
          config.enabled = val.value();
        }
      } else if (key == "min_batch_size") {
        auto val = field.value().get_uint64();
        if (!val.error()) {
          config.min_batch_size = static_cast<Uint32>(val.value());
        }
      } else if (key == "max_batch_size") {
        auto val = field.value().get_uint64();
        if (!val.error()) {
          config.max_batch_size = static_cast<Uint32>(val.value());
        }
      } else if (key == "sleep_interval_ms") {
        auto val = field.value().get_uint64();
        if (!val.error()) {
          config.sleep_interval_ms = static_cast<Uint32>(val.value());
        }
      }
    }
  } catch (...) {
    resp->setBody("{\"error\": \"Failed to parse JSON\"}");
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  // Validate config
  if (config.min_batch_size < 1) {
    config.min_batch_size = 1;
  }
  if (config.max_batch_size < config.min_batch_size) {
    config.max_batch_size = config.min_batch_size;
  }
  if (config.sleep_interval_ms < 100) {
    config.sleep_interval_ms = 100;
  }

  // Apply config
  g_ttl_purger->SetConfig(config);

  // Return updated config using RapidJSON
  rapidjson::Document respDoc;
  respDoc.SetObject();
  auto& allocator = respDoc.GetAllocator();

  respDoc.AddMember("message",
      rapidjson::Value("Configuration updated", allocator), allocator);

  rapidjson::Value configObj(rapidjson::kObjectType);
  configObj.AddMember("enabled", config.enabled, allocator);
  configObj.AddMember("min_batch_size", config.min_batch_size, allocator);
  configObj.AddMember("max_batch_size", config.max_batch_size, allocator);
  configObj.AddMember("sleep_interval_ms", config.sleep_interval_ms, allocator);
  respDoc.AddMember("config", configObj, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  respDoc.Accept(writer);

  resp->setBody(buffer.GetString());
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::getStatus(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  TTLPurgeStatus status = g_ttl_purger->GetStatus();
  resp->setBody(statusToJson(status));
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::getMetrics(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  TTLPurgeMetrics metrics = g_ttl_purger->GetMetrics();
  resp->setBody(metricsToJson(metrics));
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::getTables(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  // Get pagination parameters
  Uint32 offset = 0;
  Uint32 limit = 20;

  auto offsetParam = req->getParameter("offset");
  auto limitParam = req->getParameter("limit");

  if (!offsetParam.empty()) {
    try {
      offset = static_cast<Uint32>(std::stoul(offsetParam));
    } catch (...) {
      // Keep default
    }
  }
  if (!limitParam.empty()) {
    try {
      limit = static_cast<Uint32>(std::stoul(limitParam));
      if (limit > 100) limit = 100;  // Max 100 per page
      if (limit < 1) limit = 1;
    } catch (...) {
      // Keep default
    }
  }

  // Paginated fetch - only copies requested page, fixed cost
  Uint32 total = 0;
  std::vector<TTLTableMetrics> tables =
      g_ttl_purger->GetTableMetrics(offset, limit, &total);

  // Build JSON response (outside lock)
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  doc.AddMember("total", total, allocator);
  doc.AddMember("offset", offset, allocator);
  doc.AddMember("limit", limit, allocator);

  rapidjson::Value tablesArray(rapidjson::kArrayType);

  for (const auto& t : tables) {
    rapidjson::Value tableObj(rapidjson::kObjectType);

    tableObj.AddMember("database",
        rapidjson::Value(t.database.c_str(), allocator), allocator);
    tableObj.AddMember("table",
        rapidjson::Value(t.table.c_str(), allocator), allocator);
    tableObj.AddMember("table_id", t.table_id, allocator);
    tableObj.AddMember("ttl_sec", t.ttl_sec, allocator);
    tableObj.AddMember("ttl_column_no", t.ttl_column_no, allocator);
    tableObj.AddMember("current_partition", t.current_partition, allocator);
    tableObj.AddMember("partition_count", t.partition_count, allocator);
    tableObj.AddMember("current_batch_size", t.current_batch_size, allocator);
    tableObj.AddMember("rows_purged", (uint64_t)t.rows_purged, allocator);
    tableObj.AddMember("last_purge_time_epoch_ms",
                       (uint64_t)t.last_purge_time_epoch_ms, allocator);

    tablesArray.PushBack(tableObj, allocator);
  }

  doc.AddMember("tables", tablesArray, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  resp->setBody(buffer.GetString());
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void TTLPurgeCtrl::getTable(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback,
    const std::string &db,
    const std::string &table) {
  auto resp = drogon::HttpResponse::newHttpResponse();

  if (!checkPurgerAvailable(resp, callback)) {
    return;
  }

  // Direct lookup - O(log n), copies only single entry
  TTLTableMetrics t;
  if (g_ttl_purger->GetTableMetrics(db, table, &t)) {
    // Build JSON response (outside lock)
    resp->setBody(tableMetricsToJson(t));
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    resp->setStatusCode(drogon::HttpStatusCode::k200OK);
    callback(resp);
    return;
  }

  // Not found
  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  std::string errMsg = "Table " + db + "." + table + " not found";
  doc.AddMember("error", rapidjson::Value(errMsg.c_str(), allocator), allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  resp->setBody(buffer.GetString());
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k404NotFound);
  callback(resp);
}
