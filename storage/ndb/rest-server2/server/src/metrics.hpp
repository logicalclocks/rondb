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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_

#include "rdrs_dal.h"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>
#include <ndb_types.h>
#include <NdbTick.h>

// Classes for Updating the endpoints stats
class PkReadEndPointMetricsUpdater {
 private:
  drogon::HttpResponsePtr m_response;
  NDB_TICKS m_start_time;

 public:
  PkReadEndPointMetricsUpdater(drogon::HttpResponsePtr response);

  ~PkReadEndPointMetricsUpdater();
};

class FsReadEndPointMetricsUpdater {
 private:
  drogon::HttpResponsePtr m_response;
  NDB_TICKS m_start_time;

 public:
  FsReadEndPointMetricsUpdater(drogon::HttpResponsePtr response);

  ~FsReadEndPointMetricsUpdater();
};

class BatchPkReadEndPointMetricsUpdater {
 private:
  drogon::HttpResponsePtr m_response;
  NDB_TICKS m_start_time;
  Uint32 m_key_requests;

 public:
  BatchPkReadEndPointMetricsUpdater(drogon::HttpResponsePtr response);
  void set_key_requests(Uint32 key_requests);

  ~BatchPkReadEndPointMetricsUpdater();
};

class BatchFsReadEndPointMetricsUpdater {
 private:
  drogon::HttpResponsePtr m_response;
  NDB_TICKS m_start_time;
  Uint32 m_key_requests;

 public:
  BatchFsReadEndPointMetricsUpdater(drogon::HttpResponsePtr response);
  void set_key_requests(Uint32 key_requests);

  ~BatchFsReadEndPointMetricsUpdater();
};

class RonSQLEndPointMetricsUpdater {
 private:
  drogon::HttpResponsePtr m_response;
  NDB_TICKS m_start_time;

 public:
  RonSQLEndPointMetricsUpdater(drogon::HttpResponsePtr response);

  ~RonSQLEndPointMetricsUpdater();
};

class RondisEndPointMetricsUpdater {
 private:
  NDB_TICKS m_start_time;

 public:
  RondisEndPointMetricsUpdater();
  ~RondisEndPointMetricsUpdater();
};

class PingEndPointMetricsUpdater {
 public:
  PingEndPointMetricsUpdater();

  ~PingEndPointMetricsUpdater();
};

class HealthEndPointMetricsUpdater {
 public:
  HealthEndPointMetricsUpdater();

  ~HealthEndPointMetricsUpdater();
};


namespace rdrs_metrics {

void initMetrics();
void writeMetrics(drogon::HttpResponsePtr resp);
void setRonDBStats();

}  // namespace rdrs_metrics

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_METRICS_HPP_
