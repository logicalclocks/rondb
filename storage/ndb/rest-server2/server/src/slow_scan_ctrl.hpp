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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_SLOW_SCAN_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_SLOW_SCAN_CTRL_HPP_

#include <drogon/drogon.h>
#include <drogon/HttpController.h>

class SlowScanCtrl : public drogon::HttpController<SlowScanCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(SlowScanCtrl::getSlowScans, "/0.1.0/slow-scans", drogon::Get);
  ADD_METHOD_TO(SlowScanCtrl::clearSlowScans, "/0.1.0/slow-scans", drogon::Delete);
  METHOD_LIST_END

  static void getSlowScans(const drogon::HttpRequestPtr &req,
                           std::function<void(const drogon::HttpResponsePtr &)> &&callback);
  static void clearSlowScans(const drogon::HttpRequestPtr &req,
                             std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_SLOW_SCAN_CTRL_HPP_
