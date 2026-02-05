/*
 * Copyright (C) 2024, 2025 Hopsworks AB
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

#include "slow_scan_ctrl.hpp"
#include "scan_metrics.hpp"

#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>

void SlowScanCtrl::getSlowScans(const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  auto resp = drogon::HttpResponse::newHttpResponse();

  if (g_slow_scan_buffer == nullptr) {
    resp->setBody("{\"error\": \"Slow scan buffer not initialized\"}");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    callback(resp);
    return;
  }

  auto entries = g_slow_scan_buffer->getAll();

  rapidjson::Document doc;
  doc.SetObject();
  auto& allocator = doc.GetAllocator();

  rapidjson::Value slowScans(rapidjson::kArrayType);

  for (const auto& entry : entries) {
    rapidjson::Value scanObj(rapidjson::kObjectType);

    scanObj.AddMember("timestamp_ms", (uint64_t)entry.timestamp_ms, allocator);
    scanObj.AddMember("thread_id", (unsigned)entry.thread_id, allocator);
    scanObj.AddMember("total_us", (uint64_t)entry.timing.total_us, allocator);

    // Timing breakdown
    rapidjson::Value timings(rapidjson::kObjectType);
    timings.AddMember("json_parse_us", (uint64_t)entry.timing.json_parse_us, allocator);
    timings.AddMember("validation_us", (uint64_t)entry.timing.validation_us, allocator);
    timings.AddMember("get_ndb_object_us", (uint64_t)entry.timing.get_ndb_object_us, allocator);
    timings.AddMember("preparation_us", (uint64_t)entry.timing.preparation_us, allocator);
    timings.AddMember("start_transaction_us", (uint64_t)entry.timing.start_transaction_us, allocator);
    timings.AddMember("compile_filter_us", (uint64_t)entry.timing.compile_filter_us, allocator);
    timings.AddMember("scan_index_setup_us", (uint64_t)entry.timing.scan_index_setup_us, allocator);
    timings.AddMember("compile_index_range_us", (uint64_t)entry.timing.compile_index_range_us, allocator);
    timings.AddMember("execute_us", (uint64_t)entry.timing.execute_us, allocator);
    timings.AddMember("next_result_us", (uint64_t)entry.timing.next_result_us, allocator);
    timings.AddMember("json_serialize_us", (uint64_t)entry.timing.json_serialize_us, allocator);
    timings.AddMember("close_operation_us", (uint64_t)entry.timing.close_operation_us, allocator);
    timings.AddMember("return_ndb_object_us", (uint64_t)entry.timing.return_ndb_object_us, allocator);
    timings.AddMember("callback_us", (uint64_t)entry.timing.callback_us, allocator);
    scanObj.AddMember("timings", timings, allocator);

    // Context
    rapidjson::Value context(rapidjson::kObjectType);
    context.AddMember("database",
      rapidjson::Value(entry.timing.database.c_str(), allocator), allocator);
    context.AddMember("table",
      rapidjson::Value(entry.timing.table.c_str(), allocator), allocator);
    context.AddMember("index_name",
      rapidjson::Value(entry.timing.index_name.c_str(), allocator), allocator);
    context.AddMember("rows_fetched", (uint64_t)entry.timing.rows_fetched, allocator);
    context.AddMember("limit", (uint64_t)entry.timing.limit, allocator);
    context.AddMember("has_filter", entry.timing.has_filter, allocator);
    context.AddMember("is_index_scan", entry.timing.is_index_scan, allocator);
    scanObj.AddMember("context", context, allocator);

    slowScans.PushBack(scanObj, allocator);
  }

  // Add aggregated statistics
  AggregatedScanStats stats = getAggregatedScanStats();
  rapidjson::Value statsObj(rapidjson::kObjectType);
  statsObj.AddMember("total_scans", (uint64_t)stats.total_count, allocator);
  statsObj.AddMember("avg_us", (uint64_t)stats.getAvg(), allocator);
  statsObj.AddMember("p80_us", (uint64_t)stats.getP80(), allocator);
  statsObj.AddMember("p90_us", (uint64_t)stats.getP90(), allocator);
  statsObj.AddMember("p95_us", (uint64_t)stats.getP95(), allocator);
  statsObj.AddMember("p99_us", (uint64_t)stats.getP99(), allocator);
  statsObj.AddMember("p100_us", (uint64_t)stats.getP100(), allocator);

  // Add histogram bucket distribution
  rapidjson::Value histogram(rapidjson::kObjectType);
  histogram.AddMember("0-100us", (uint64_t)stats.buckets[0], allocator);
  histogram.AddMember("100-500us", (uint64_t)stats.buckets[1], allocator);
  histogram.AddMember("500us-1ms", (uint64_t)stats.buckets[2], allocator);
  histogram.AddMember("1-2ms", (uint64_t)stats.buckets[3], allocator);
  histogram.AddMember("2-5ms", (uint64_t)stats.buckets[4], allocator);
  histogram.AddMember("5-10ms", (uint64_t)stats.buckets[5], allocator);
  histogram.AddMember("10-20ms", (uint64_t)stats.buckets[6], allocator);
  histogram.AddMember("20-50ms", (uint64_t)stats.buckets[7], allocator);
  histogram.AddMember("50-100ms", (uint64_t)stats.buckets[8], allocator);
  histogram.AddMember("100-500ms", (uint64_t)stats.buckets[9], allocator);
  histogram.AddMember("500ms-1s", (uint64_t)stats.buckets[10], allocator);
  histogram.AddMember(">1s", (uint64_t)stats.buckets[11], allocator);
  statsObj.AddMember("histogram", histogram, allocator);

  doc.AddMember("statistics", statsObj, allocator);

  doc.AddMember("slow_scans", slowScans, allocator);
  doc.AddMember("count", (uint64_t)entries.size(), allocator);
  doc.AddMember("total_count", (uint64_t)g_slow_scan_buffer->totalCount(), allocator);
  doc.AddMember("threshold_us", (uint64_t)g_slow_scan_threshold_us, allocator);
  doc.AddMember("buffer_size", (uint64_t)g_slow_scan_buffer_size, allocator);
  doc.AddMember("enabled", g_scan_timing_enabled, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  resp->setBody(buffer.GetString());
  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}

void SlowScanCtrl::clearSlowScans(const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {

  auto resp = drogon::HttpResponse::newHttpResponse();

  if (g_slow_scan_buffer != nullptr) {
    g_slow_scan_buffer->clear();
    clearAllScanStats();
    resp->setBody("{\"status\": \"cleared\"}");
  } else {
    resp->setBody("{\"error\": \"Slow scan buffer not initialized\"}");
  }

  resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  callback(resp);
}
