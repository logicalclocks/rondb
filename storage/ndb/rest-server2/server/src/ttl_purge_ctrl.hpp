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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_CTRL_HPP_

#include "constants.hpp"
#include "ttl_purge.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>

class TTLPurgeCtrl : public drogon::HttpController<TTLPurgeCtrl> {
 public:
  METHOD_LIST_BEGIN
  // GET /0.1.0/ttl-purge - Combined view (config + status + metrics)
  ADD_METHOD_TO(TTLPurgeCtrl::getAll, TTL_PURGE_PATH, drogon::Get);
  // GET /0.1.0/ttl-purge/config - Get configuration
  ADD_METHOD_TO(TTLPurgeCtrl::getConfig, TTL_PURGE_CONFIG_PATH, drogon::Get);
  // PUT /0.1.0/ttl-purge/config - Update configuration
  ADD_METHOD_TO(TTLPurgeCtrl::updateConfig, TTL_PURGE_CONFIG_PATH, drogon::Put);
  // GET /0.1.0/ttl-purge/status - Get status
  ADD_METHOD_TO(TTLPurgeCtrl::getStatus, TTL_PURGE_STATUS_PATH, drogon::Get);
  // GET /0.1.0/ttl-purge/metrics - Get metrics
  ADD_METHOD_TO(TTLPurgeCtrl::getMetrics, TTL_PURGE_METRICS_PATH, drogon::Get);
  // GET /0.1.0/ttl-purge/tables - Get all table metrics (paginated)
  ADD_METHOD_TO(TTLPurgeCtrl::getTables, TTL_PURGE_TABLES_PATH, drogon::Get);
  // GET /0.1.0/ttl-purge/tables/{db}/{table} - Get single table metrics
  ADD_METHOD_TO(TTLPurgeCtrl::getTable, TTL_PURGE_TABLE_PATH, drogon::Get);
  METHOD_LIST_END

  // Handler methods
  static void getAll(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void getConfig(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void updateConfig(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void getStatus(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void getMetrics(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void getTables(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);

  static void getTable(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback,
      const std::string &db,
      const std::string &table);

 private:
  // Helper methods for JSON generation
  static std::string configToJson(const TTLPurgeConfig &config);
  static std::string statusToJson(const TTLPurgeStatus &status);
  static std::string metricsToJson(const TTLPurgeMetrics &metrics);
  static std::string tableMetricsToJson(const TTLTableMetrics &table);
  static const char* stateToString(TTLPurgeStatus::State state);
};

// Global TTL purger instance (defined in main.cc)
extern TTLPurger* g_ttl_purger;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_TTL_PURGE_CTRL_HPP_
