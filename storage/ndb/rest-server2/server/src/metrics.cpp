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

#include "prometheus_ctrl.hpp"
#include "rdrs_dal.h"
#include "logger.hpp"

#include <drogon/HttpTypes.h>
#include <prometheus/text_serializer.h>
#include <prometheus/counter.h>
#include <prometheus/summary.h>
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/info.h>
#include <prometheus/registry.h>
#include <string>
#include <vector>
#include <my_compiler.h>  // for likely and unlikely
#include <metrics.hpp>

using namespace prometheus;

/* Much of this code deals with accumulating metrics into intermediate variables
 * in a fast way. These intermediate variables are defined below, named
 * *_counter, *_histogram and *_histogram_boundaries. They are initialized in
 * init_metrics_intermediate_variables().
 *
 * Only when the /metrics endpoint is called, will prometheus
 * data be updated through e.g. prometheus::Histogram::Observe calls.
 */

/* Number of NDB key requests contained in finished batch and
 * batch_feature_store HTTP requests, respectively. We do not need a similar
 * variable for pk-read or fs, since that can already be determined from the
 * pk_read_histogram and fs_histogram totals.
 * Todo: Track number of NDB key lookups caused by Rondis.
*/
std::atomic<Uint64> m_ndb_key_request_from_batch_counter;
std::atomic<Uint64> m_ndb_key_request_from_fs_batch_counter;
/*
 * Number of started ping HTTP requests.
 */
std::atomic<Uint64> m_ping_request_counter;
/*
 * Number of started health HTTP requests.
 */
std::atomic<Uint64> m_health_request_counter;
/*
 * Number of finished metrics HTTP requests.
 */
std::atomic<Uint64> m_metrics_request_counter;

/*
 * Histogram count buckets.
 * - Indexes 0-60 hold counts of successful requests in buckets defined by
 *   *_histogram_boundaries variables below. This means that index 60 holds the
 *   overflow bucket, which has no upper bound.
 * - Index 61 holds number of HTTP 400 responses.
 * - Index 62 holds number of HTTP 500 responses.
 * - Index 63 holds number of HTTP error responses other than 400 & 500.
 */
std::atomic<Uint64> pk_read_histogram[64];
std::atomic<Uint64> batch_pk_read_histogram[64];
std::atomic<Uint64> fs_histogram[64];
std::atomic<Uint64> batch_fs_histogram[64];
std::atomic<Uint64> ronsql_histogram[64];
std::atomic<Uint64> index_scan_histogram[64];
std::atomic<Uint64> rondis_histogram[61]; // not 64, rondis errors not counted

/*
 * Histogram totals. This holds the totals of all latencies with precision, so
 * that we do not need to rely on heuristics based on bucket limits.
 */
std::atomic<Uint64> pk_read_histogram_total;
std::atomic<Uint64> batch_pk_read_histogram_total;
std::atomic<Uint64> fs_histogram_total;
std::atomic<Uint64> batch_fs_histogram_total;
std::atomic<Uint64> ronsql_histogram_total;
std::atomic<Uint64> index_scan_histogram_total;
std::atomic<Uint64> rondis_histogram_total;

/*
 * Number of rows fetched by finished index scan HTTP requests.
 */
std::atomic<Uint64> m_rows_fetched_from_index_scan_counter;

/*
 * Histogram boundaries. NAME_histogram[i] holds the number of requests such
 * that
 *   NAME_histogram_boundaries[i-1] <= latency < NAME_histogram_boundaries[i]
 * (where 0 is used in place of NAME_histogram_boundars[-1], and +Inf is used in
 * place of NAME_histogram_boundars[60])
 * The prometheus histogram will however interpret the bucket as containing the
 * number of requests such that
 *   NAME_histogram_boundaries[i-1] < latency <= NAME_histogram_boundaries[i]
 * which means that we technically overestimate all latencies by 1 µs.
 */
Uint32 pk_read_histogram_boundaries[60];
Uint32 batch_pk_read_histogram_boundaries[60];
Uint32 fs_histogram_boundaries[60];
Uint32 batch_fs_histogram_boundaries[60];
Uint32 ronsql_histogram_boundaries[60];
Uint32 index_scan_histogram_boundaries[60];
Uint32 rondis_histogram_boundaries[60];

static void
init_metrics_intermediate_variables() {
  m_ndb_key_request_from_batch_counter = 0;
  m_ndb_key_request_from_fs_batch_counter = 0;
  m_ping_request_counter = 0;
  m_health_request_counter = 0;
  m_metrics_request_counter = 0;
  for (Uint32 i = 0; i < 64; i++) {
    pk_read_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 64; i++) {
    batch_pk_read_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 64; i++) {
    fs_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 64; i++) {
    batch_fs_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 64; i++) {
    ronsql_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 64; i++) {
    index_scan_histogram[i] = 0;
  }
  for (Uint32 i = 0; i < 61 /* not 64, rondis errors not counted */ ; i++) {
    rondis_histogram[i] = 0;
  }
  pk_read_histogram_total = 0;
  batch_pk_read_histogram_total = 0;
  fs_histogram_total = 0;
  batch_fs_histogram_total = 0;
  ronsql_histogram_total = 0;
  index_scan_histogram_total = 0;
  m_rows_fetched_from_index_scan_counter = 0;
  rondis_histogram_total = 0;

  for (Uint32 i = 0; i < 10; i++) {
    pk_read_histogram_boundaries[i] = 40 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    pk_read_histogram_boundaries[10 + i] = 400 + 100 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    pk_read_histogram_boundaries[20 + i] = 1400 + 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    pk_read_histogram_boundaries[30 + i] = 3400 + 260 * (i + 1);
  }
  Uint32 pk_boundary = 7500;
  for (Uint32 i = 0; i < 20; i++) {
    pk_read_histogram_boundaries[40 + i] = pk_boundary;
    pk_boundary = (pk_boundary / 2) * 3;
  }

  for (Uint32 i = 0; i < 20; i++) {
    batch_pk_read_histogram_boundaries[i] = 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 20; i++) {
    batch_pk_read_histogram_boundaries[20 + i] = 4000 + 500 * (i + 1);
  }
  Uint32 batch_pk_boundary = 21000;
  for (Uint32 i = 0; i < 20; i++) {
    batch_pk_read_histogram_boundaries[40 + i] = batch_pk_boundary;
    batch_pk_boundary = (batch_pk_boundary / 2) * 3;
  }

  for (Uint32 i = 0; i < 10; i++) {
    fs_histogram_boundaries[i] = 50 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    fs_histogram_boundaries[10 + i] = 500 + 100 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    fs_histogram_boundaries[20 + i] = 1500 + 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    fs_histogram_boundaries[30 + i] = 3500 + 300 * (i + 1);
  }
  Uint32 fs_boundary = 9750;
  for (Uint32 i = 0; i < 20; i++) {
    fs_histogram_boundaries[40 + i] = fs_boundary;
    fs_boundary = (fs_boundary / 2) * 3;
  }

  for (Uint32 i = 0; i < 20; i++) {
    batch_fs_histogram_boundaries[i] = 250 * (i + 1);
  }
  for (Uint32 i = 0; i < 20; i++) {
    batch_fs_histogram_boundaries[20 + i] = 5000 + 600 * (i + 1);
  }
  Uint32 batch_fs_boundary = 25500;
  for (Uint32 i = 0; i < 20; i++) {
    batch_fs_histogram_boundaries[40 + i] = batch_fs_boundary;
    batch_fs_boundary = (batch_fs_boundary / 2) * 3;
  }

  for (Uint32 i = 0; i < 10; i++) {
    ronsql_histogram_boundaries[i] = 100 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    ronsql_histogram_boundaries[10 + i] = 1000 + 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    ronsql_histogram_boundaries[20 + i] = 3000 + 500 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    ronsql_histogram_boundaries[30 + i] = 8000 + 2000 * (i + 1);
  }
  Uint32 ronsql_boundary = 72000;
  for (Uint32 i = 0; i < 20; i++) {
    ronsql_histogram_boundaries[40 + i] = ronsql_boundary;
    ronsql_boundary = (ronsql_boundary / 2) * 3;
  }

  // Index scan uses same boundaries as ronsql
  for (Uint32 i = 0; i < 10; i++) {
    index_scan_histogram_boundaries[i] = 100 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    index_scan_histogram_boundaries[10 + i] = 1000 + 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    index_scan_histogram_boundaries[20 + i] = 3000 + 500 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    index_scan_histogram_boundaries[30 + i] = 8000 + 2000 * (i + 1);
  }
  Uint32 index_scan_boundary = 72000;
  for (Uint32 i = 0; i < 20; i++) {
    index_scan_histogram_boundaries[40 + i] = index_scan_boundary;
    index_scan_boundary = (index_scan_boundary / 2) * 3;
  }

  for (Uint32 i = 0; i < 10; i++) {
    rondis_histogram_boundaries[i] = 40 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    rondis_histogram_boundaries[10 + i] = 400 + 100 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    rondis_histogram_boundaries[20 + i] = 1400 + 200 * (i + 1);
  }
  for (Uint32 i = 0; i < 10; i++) {
    rondis_histogram_boundaries[30 + i] = 3400 + 260 * (i + 1);
  }
  Uint32 rondis_boundary = 7500;
  for (Uint32 i = 0; i < 20; i++) {
    rondis_histogram_boundaries[40 + i] = rondis_boundary;
    rondis_boundary = (rondis_boundary / 2) * 3;
  }
}

static Uint32 calculate_pk_index(Uint64 micros) {
  if (micros < Uint64(400)) {
    return micros / Uint64(40);
  } else if (micros < Uint64(1400)) {
    return ((micros - Uint64(400)) / Uint64(100)) + 10;
  } else if (micros < Uint64(3400)) {
    return ((micros - Uint64(1400)) / Uint64(200)) + 20;
  } else if (micros < Uint64(6000)) {
    return ((micros - Uint64(3400)) / Uint64(260)) + 30;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < pk_read_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

static Uint32 calculate_batch_pk_index(Uint64 micros) {
  if (micros < Uint64(4000)) {
    return micros / Uint64(200);
  } else if (micros < Uint64(14000)) {
    return ((micros - Uint64(4000)) / Uint64(500)) + 20;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < batch_pk_read_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

static Uint32 calculate_fs_index(Uint64 micros) {
  if (micros < Uint64(500)) {
    return micros / Uint64(50);
  } else if (micros < Uint64(1500)) {
    return ((micros - Uint64(500)) / Uint64(100)) + 10;
  } else if (micros < Uint64(3500)) {
    return ((micros - Uint64(1500)) / Uint64(200)) + 20;
  } else if (micros < Uint64(6500)) {
    return ((micros - Uint64(3500)) / Uint64(300)) + 30;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < fs_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

static Uint32 calculate_batch_fs_index(Uint64 micros) {
  if (micros < Uint64(5000)) {
    return micros / Uint64(250);
  } else if (micros < Uint64(17000)) {
    return ((micros - Uint64(5000)) / Uint64(600)) + 20;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < batch_fs_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

static Uint32 calculate_ronsql_index(Uint64 micros) {
  if (micros < Uint64(1000)) {
    return micros / Uint64(100);
  } else if (micros < Uint64(3000)) {
    return ((micros - Uint64(1000)) / Uint64(200)) + 10;
  } else if (micros < Uint64(8000)) {
    return ((micros - Uint64(3000)) / Uint64(500)) + 20;
  } else if (micros < Uint64(28000)) {
    return ((micros - Uint64(8000)) / Uint64(2000)) + 30;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < ronsql_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

// Index scan uses same bucket calculation as ronsql
static Uint32 calculate_index_scan_index(Uint64 micros) {
  if (micros < Uint64(1000)) {
    return micros / Uint64(100);
  } else if (micros < Uint64(3000)) {
    return ((micros - Uint64(1000)) / Uint64(200)) + 10;
  } else if (micros < Uint64(8000)) {
    return ((micros - Uint64(3000)) / Uint64(500)) + 20;
  } else if (micros < Uint64(28000)) {
    return ((micros - Uint64(8000)) / Uint64(2000)) + 30;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < index_scan_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

static Uint32 calculate_rondis_index(Uint64 micros) {
  if (micros < Uint64(400)) {
    return micros / Uint64(40);
  } else if (micros < Uint64(1400)) {
    return ((micros - Uint64(400)) / Uint64(100)) + 10;
  } else if (micros < Uint64(3400)) {
    return ((micros - Uint64(1400)) / Uint64(200)) + 20;
  } else if (micros < Uint64(6000)) {
    return ((micros - Uint64(3400)) / Uint64(260)) + 30;
  } else {
    for (Uint32 i = 40; i < 60; i++) {
      if (micros < rondis_histogram_boundaries[i]) return i;
    }
    return Uint32(60);
  }
}

PkReadEndPointMetricsUpdater::PkReadEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
}

PkReadEndPointMetricsUpdater::~PkReadEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  int code = static_cast<int>(m_response->getStatusCode());
  Uint32 hist;
  if (code >= 200 && code < 300) {
    hist = calculate_pk_index(elapsed_us);
    pk_read_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
  } else if (code >= 400 && code < 500) {
    hist = 61;  // All 4xx client errors
  } else if (code >= 500) {
    hist = 62;  // All 5xx server errors
  } else {
    hist = 63;  // 1xx, 3xx
  }
  pk_read_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}

BatchPkReadEndPointMetricsUpdater::BatchPkReadEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
  m_key_requests = 0;
}

void
BatchPkReadEndPointMetricsUpdater::set_key_requests(Uint32 key_requests) {
  m_key_requests = key_requests;
}

BatchPkReadEndPointMetricsUpdater::~BatchPkReadEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  int code = static_cast<int>(m_response->getStatusCode());
  Uint32 hist;
  if (code >= 200 && code < 300) {
    hist = calculate_batch_pk_index(elapsed_us);
    batch_pk_read_histogram_total.fetch_add(
      elapsed_us,
      std::memory_order_relaxed);
    m_ndb_key_request_from_batch_counter.fetch_add(
      m_key_requests,
      std::memory_order_relaxed);
  } else if (code >= 400 && code < 500) {
    hist = 61;  // All 4xx client errors
  } else if (code >= 500) {
    hist = 62;  // All 5xx server errors
  } else {
    hist = 63;  // 1xx, 3xx
  }
  batch_pk_read_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}


FsReadEndPointMetricsUpdater::FsReadEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
}

FsReadEndPointMetricsUpdater::~FsReadEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  int code = static_cast<int>(m_response->getStatusCode());
  Uint32 hist;
  if (code >= 200 && code < 300) {
    hist = calculate_fs_index(elapsed_us);
    fs_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
  } else if (code >= 400 && code < 500) {
    hist = 61;  // All 4xx client errors
  } else if (code >= 500) {
    hist = 62;  // All 5xx server errors
  } else {
    hist = 63;  // 1xx, 3xx
  }
  fs_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}

BatchFsReadEndPointMetricsUpdater::BatchFsReadEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
  m_key_requests = 0;
}

void
BatchFsReadEndPointMetricsUpdater::set_key_requests(Uint32 key_requests) {
  m_key_requests = key_requests;
}

BatchFsReadEndPointMetricsUpdater::~BatchFsReadEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  int code = static_cast<int>(m_response->getStatusCode());
  Uint32 hist;
  if (code >= 200 && code < 300) {
    hist = calculate_batch_fs_index(elapsed_us);
    batch_fs_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
    m_ndb_key_request_from_fs_batch_counter.fetch_add(
      m_key_requests,
      std::memory_order_relaxed);
  } else if (code >= 400 && code < 500) {
    hist = 61;  // All 4xx client errors
  } else if (code >= 500) {
    hist = 62;  // All 5xx server errors
  } else {
    hist = 63;  // 1xx, 3xx
  }
  batch_fs_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}

RonSQLEndPointMetricsUpdater::RonSQLEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
}

RonSQLEndPointMetricsUpdater::~RonSQLEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  int code = static_cast<int>(m_response->getStatusCode());
  Uint32 hist;
  if (code >= 200 && code < 300) {
    hist = calculate_ronsql_index(elapsed_us);
    ronsql_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
  } else if (code >= 400 && code < 500) {
    hist = 61;  // All 4xx client errors
  } else if (code >= 500) {
    hist = 62;  // All 5xx server errors
  } else {
    hist = 63;  // 1xx, 3xx
  }
  ronsql_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}

IndexScanEndPointMetricsUpdater::IndexScanEndPointMetricsUpdater(
  drogon::HttpResponsePtr response) {
  m_start_time = NdbTick_getCurrentTicks();
  m_response = response;
  m_rows_fetched = 0;
}

void
IndexScanEndPointMetricsUpdater::set_rows_fetched(Uint64 rows_fetched) {
  m_rows_fetched = rows_fetched;
}

IndexScanEndPointMetricsUpdater::~IndexScanEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  auto status = m_response->getStatusCode();
  Uint32 hist;
  if (status == drogon::HttpStatusCode::k200OK) {
    hist = calculate_index_scan_index(elapsed_us);
    index_scan_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
    m_rows_fetched_from_index_scan_counter.fetch_add(
      m_rows_fetched,
      std::memory_order_relaxed);
  } else if (status == drogon::HttpStatusCode::k400BadRequest) {
    hist = 61;
  } else if (status == drogon::HttpStatusCode::k500InternalServerError) {
    hist = 62;
  } else {
    hist = 63; // Other error
  }
  index_scan_histogram[hist].fetch_add(1, std::memory_order_relaxed);
}

RondisEndPointMetricsUpdater::RondisEndPointMetricsUpdater() {
  m_start_time = NdbTick_getCurrentTicks();
}

RondisEndPointMetricsUpdater::~RondisEndPointMetricsUpdater() {
  NDB_TICKS now = NdbTick_getCurrentTicks();
  Uint64 elapsed_us = NdbTick_Elapsed(m_start_time, now).microSec();
  Uint32 hist = calculate_rondis_index(elapsed_us);
  rondis_histogram[hist].fetch_add(1, std::memory_order_relaxed);
  rondis_histogram_total.fetch_add(elapsed_us, std::memory_order_relaxed);
}

PingEndPointMetricsUpdater::PingEndPointMetricsUpdater() {
  m_ping_request_counter.fetch_add(1, std::memory_order_relaxed);
}

PingEndPointMetricsUpdater::~PingEndPointMetricsUpdater() {
}

HealthEndPointMetricsUpdater::HealthEndPointMetricsUpdater() {
  m_health_request_counter.fetch_add(1, std::memory_order_relaxed);
}

HealthEndPointMetricsUpdater::~HealthEndPointMetricsUpdater() {
}

MetricsEndPointMetricsUpdater::MetricsEndPointMetricsUpdater() {
  m_metrics_request_counter.fetch_add(1, std::memory_order_relaxed);
}

MetricsEndPointMetricsUpdater::~MetricsEndPointMetricsUpdater() {
}

namespace rdrs_metrics {

namespace {

std::shared_ptr<Registry> registry = std::make_shared<Registry>();

prometheus::Counter *pkReadCounter = nullptr;
prometheus::Counter *pkReadCounter400 = nullptr;
prometheus::Counter *pkReadCounter500 = nullptr;
prometheus::Counter *pkReadCounterOther = nullptr;

prometheus::Counter *batchPkReadCounter = nullptr;
prometheus::Counter *batchPkReadCounter400 = nullptr;
prometheus::Counter *batchPkReadCounter500 = nullptr;
prometheus::Counter *batchPkReadCounterOther = nullptr;

prometheus::Counter *fsReadCounter = nullptr;
prometheus::Counter *fsReadCounter400 = nullptr;
prometheus::Counter *fsReadCounter500 = nullptr;
prometheus::Counter *fsReadCounterOther = nullptr;

prometheus::Counter *batchFsReadCounter = nullptr;
prometheus::Counter *batchFsReadCounter400 = nullptr;
prometheus::Counter *batchFsReadCounter500 = nullptr;
prometheus::Counter *batchFsReadCounterOther = nullptr;

prometheus::Counter *ronSQLReadCounter = nullptr;
prometheus::Counter *ronSQLReadCounter400 = nullptr;
prometheus::Counter *ronSQLReadCounter500 = nullptr;
prometheus::Counter *ronSQLReadCounterOther = nullptr;

prometheus::Counter *indexScanReadCounter = nullptr;
prometheus::Counter *indexScanReadCounter400 = nullptr;
prometheus::Counter *indexScanReadCounter500 = nullptr;
prometheus::Counter *indexScanReadCounterOther = nullptr;

prometheus::Counter *rondisCmdCounter = nullptr;

prometheus::Counter *ndbKeyRequestFromPkReadCounter = nullptr;
prometheus::Counter *ndbKeyRequestFromBatchCounter = nullptr;
prometheus::Counter *ndbKeyRequestFromFsCounter = nullptr;
prometheus::Counter *ndbKeyRequestFromBatchFsCounter = nullptr;
prometheus::Counter *rowsFetchedFromIndexScanCounter = nullptr;
prometheus::Counter *pingCounter = nullptr;
prometheus::Counter *healthCounter = nullptr;
prometheus::Counter *metricsCounter = nullptr;

prometheus::Histogram *pkReadHistogram = nullptr;
prometheus::Histogram *batchPkReadHistogram = nullptr;
prometheus::Histogram *fsReadHistogram = nullptr;
prometheus::Histogram *batchFsReadHistogram = nullptr;
prometheus::Histogram *ronSQLReadHistogram = nullptr;
prometheus::Histogram *indexScanReadHistogram = nullptr;
prometheus::Histogram *rondisHistogram = nullptr;

prometheus::Gauge *ronDBConnectionStateGauge            = nullptr;
prometheus::Gauge *ndbObjectsTotalCountGauge            = nullptr;
prometheus::Gauge *httpConnectionCountGauge             = nullptr;
}  // namespace

void initMetrics() {
  init_metrics_intermediate_variables();

  /* Request Counters (RDRS, Rondis and NDB) */
  prometheus::Family<prometheus::Counter> *requestCounter =
    &BuildCounter()
      .Name("rdrs_endpoints_response_status_count")
      .Help("Number of response status returned by REST API")
      .Register(*registry);

  /* RDRS pk-read Request Counters */
  pkReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", PKREAD},
                          {"method", POST},
                          {"status", "200"}});

  pkReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", PKREAD},
                          {"method", POST},
                          {"status", "400"}});

  pkReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", PKREAD},
                          {"method", POST},
                          {"status", "500"}});

  pkReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", PKREAD},
                          {"method", POST},
                          {"status", "other"}});


  /* RDRS batch Request Counters */
  batchPkReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH},
                          {"method", POST},
                          {"status", "200"}});

  batchPkReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH},
                          {"method", POST},
                          {"status", "400"}});

  batchPkReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH},
                          {"method", POST},
                          {"status", "500"}});

  batchPkReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH},
                          {"method", POST},
                          {"status", "other"}});


  /* RDRS feature_store Request Counters */
  fsReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", FEATURE_STORE},
                          {"method", POST},
                          {"status", "200"}});

  fsReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", FEATURE_STORE},
                          {"method", POST},
                          {"status", "400"}});

  fsReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", FEATURE_STORE},
                          {"method", POST},
                          {"status", "500"}});

  fsReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", FEATURE_STORE},
                          {"method", POST},
                          {"status", "other"}});


  /* RDRS batch_feature_store Request Counters */
  batchFsReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH_FEATURE_STORE},
                          {"method", POST},
                          {"status", "200"}});

  batchFsReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH_FEATURE_STORE},
                          {"method", POST},
                          {"status", "400"}});

  batchFsReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH_FEATURE_STORE},
                          {"method", POST},
                          {"status", "500"}});

  batchFsReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", BATCH_FEATURE_STORE},
                          {"method", POST},
                          {"status", "other"}});


  /* RDRS ronsql Request Counters */
  ronSQLReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", RONSQL},
                          {"method", POST},
                          {"status", "200"}});

  ronSQLReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", RONSQL},
                          {"method", POST},
                          {"status", "400"}});

  ronSQLReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", RONSQL},
                          {"method", POST},
                          {"status", "500"}});

  ronSQLReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", RONSQL},
                          {"method", POST},
                          {"status", "other"}});

  /* RDRS index scan Request Counters */
  indexScanReadCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", SCAN},
                          {"method", POST},
                          {"status", "200"}});

  indexScanReadCounter400 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", SCAN},
                          {"method", POST},
                          {"status", "400"}});

  indexScanReadCounter500 =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", SCAN},
                          {"method", POST},
                          {"status", "500"}});

  indexScanReadCounterOther =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", SCAN},
                          {"method", POST},
                          {"status", "other"}});

  /* Rondis Request Counter */
  rondisCmdCounter =
    &requestCounter->Add({{"api_type", "Rondis"},
                          {"endpoint", "Rondis"},
                          {"method", "Rondis"}});

  /* NDB Key Request Counters */
  ndbKeyRequestFromPkReadCounter =
    &requestCounter->Add({{"api_type", "NDB"},
                          {"caused_by_endpoint", PKREAD},
                          {"endpoint", "key"}});
  ndbKeyRequestFromBatchCounter =
    &requestCounter->Add({{"api_type", "NDB"},
                          {"caused_by_endpoint", BATCH},
                          {"endpoint", "key"}});
  ndbKeyRequestFromFsCounter =
    &requestCounter->Add({{"api_type", "NDB"},
                          {"caused_by_endpoint", FEATURE_STORE},
                          {"endpoint", "key"}});
  ndbKeyRequestFromBatchFsCounter =
    &requestCounter->Add({{"api_type", "NDB"},
                          {"caused_by_endpoint", BATCH_FEATURE_STORE},
                          {"endpoint", "key"}});

  /* Rows fetched from index scan */
  rowsFetchedFromIndexScanCounter =
    &requestCounter->Add({{"api_type", "NDB"},
                          {"caused_by_endpoint", SCAN},
                          {"endpoint", "rows_fetched"}});

  /* RDRS ping Request Counter */
  pingCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", PING},
                          {"method", POST},
                          {"status", "200"}});

  /* RDRS health Request Counters */
  healthCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", HEALTH},
                          {"method", POST},
                          {"status", "200"}});

  /* RDRS metrics Request Counters */
  metricsCounter =
    &requestCounter->Add({{"api_type", "REST"},
                          {"endpoint", METRICS},
                          {"method", POST},
                          {"status", "200"}});

  prometheus::Family<prometheus::Histogram>& request_duration =
    prometheus::BuildHistogram()
      .Name("http_request_duration_seconds")
      .Help("Histogram of response times")
      .Register(*registry);
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)pk_read_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    pkReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", PKREAD}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)batch_pk_read_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    batchPkReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", BATCH}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)fs_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    fsReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", FEATURE_STORE}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)batch_fs_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    batchFsReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", BATCH_FEATURE_STORE}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)ronsql_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    ronSQLReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", RONSQL}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)index_scan_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    indexScanReadHistogram =
      &request_duration.Add({{"api_type", "REST"},
                             {"method", "POST"},
                             {"endpoint", SCAN}},
        hist_boundaries);
  }
  {
    std::vector<double> hist_boundaries(60);
    for (Uint32 i = 0; i < 60; i++) {
      double hist_boundary = double(1);
      hist_boundary *= (double)rondis_histogram_boundaries[i];
      hist_boundary /= (double)1000000;
      hist_boundaries[i] = hist_boundary;
    }
    // Rondis does not use HTTP. Use placeholder values for method and endpoint.
    rondisHistogram =
      &request_duration.Add({{"api_type", "Rondis"},
                             {"method", "Rondis"},
                             {"endpoint", "Rondis"}},
        hist_boundaries);
  }

  ronDBConnectionStateGauge = &prometheus::BuildGauge()
    .Name("rdrs_rondb_connection_state")
    .Help("Connection state (0: connected, > 0  not connected)")
    .Register(*registry)
    .Add({});

  ndbObjectsTotalCountGauge = &prometheus::BuildGauge()
    .Name("rdrs_rondb_total_ndb_objects")
    .Help("Total NDB objects")
    .Register(*registry)
    .Add({});

  httpConnectionCountGauge = &prometheus::BuildGauge()
    .Name("rdrs_http_connection_count")
    .Help("Number of currently active HTTP connections")
    .Register(*registry)
    .Add({});
}

void setRonDBStats() {
  RonDB_Stats stats;
  RS_Status status = get_rondb_stats(&stats);

  if (likely(status.http_code == SUCCESS)) {
    ndbObjectsTotalCountGauge->Set(stats.ndb_objects_count);
    ronDBConnectionStateGauge->Set(stats.connection_state);
  } else {
    rdrs_logger::error("Failed to read metrics for RonDB");
  }
}

void writeMetrics(drogon::HttpResponsePtr resp) {

  // pk-read
  Uint64 hist_counters[64];
  std::vector<double> hist_counters_dbl(61);
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count = pk_read_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  Uint64 tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    pkReadCounter->Increment(tot_count);
    ndbKeyRequestFromPkReadCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    pkReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    pkReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    pkReadCounterOther->Increment(hist_counters[63]);
  }
  Uint64 tot_value = pk_read_histogram_total.exchange(
    0,
    std::memory_order_relaxed);
  pkReadHistogram->ObserveMultiple(hist_counters_dbl,
                                   (double)tot_value / (double)1000000);

  // batch
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count =
      batch_pk_read_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    batchPkReadCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    batchPkReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    batchPkReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    batchPkReadCounterOther->Increment(hist_counters[63]);
  }
  tot_value = batch_pk_read_histogram_total.exchange(
    0,
    std::memory_order_relaxed);
  batchPkReadHistogram->ObserveMultiple(hist_counters_dbl,
                                        (double)tot_value / (double)1000000);

  // feature_store
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count = fs_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    fsReadCounter->Increment(tot_count);
    ndbKeyRequestFromFsCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    fsReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    fsReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    fsReadCounterOther->Increment(hist_counters[63]);
  }
  tot_value = fs_histogram_total.exchange(0, std::memory_order_relaxed);
  fsReadHistogram->ObserveMultiple(hist_counters_dbl,
                                   (double)tot_value / (double)1000000);

  // batch_feature_store
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count =
      batch_fs_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    batchFsReadCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    batchFsReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    batchFsReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    batchFsReadCounterOther->Increment(hist_counters[63]);
  }
  tot_value = batch_fs_histogram_total.exchange(0, std::memory_order_relaxed);
  batchFsReadHistogram->ObserveMultiple(hist_counters_dbl,
                                        (double)tot_value / (double)1000000);

  // ronsql
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count = ronsql_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    ronSQLReadCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    ronSQLReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    ronSQLReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    ronSQLReadCounterOther->Increment(hist_counters[63]);
  }
  tot_value = ronsql_histogram_total.exchange(0, std::memory_order_relaxed);
  ronSQLReadHistogram->ObserveMultiple(hist_counters_dbl,
                                       (double)tot_value / (double)1000000);

  // index_scan
  for (Uint32 i = 0; i < 64; i++) {
    Uint64 count = index_scan_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    if (i < 61)
      hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    indexScanReadCounter->Increment(tot_count);
  }
  if (hist_counters[61] > 0) {
    indexScanReadCounter400->Increment(hist_counters[61]);
  }
  if (hist_counters[62] > 0) {
    indexScanReadCounter500->Increment(hist_counters[62]);
  }
  if (hist_counters[63] > 0) {
    indexScanReadCounterOther->Increment(hist_counters[63]);
  }
  tot_value = index_scan_histogram_total.exchange(0, std::memory_order_relaxed);
  indexScanReadHistogram->ObserveMultiple(hist_counters_dbl,
                                          (double)tot_value / (double)1000000);

  // rows fetched from index scan
  Uint64 rows_fetched_count = m_rows_fetched_from_index_scan_counter.exchange(
    0,
    std::memory_order_relaxed);
  rowsFetchedFromIndexScanCounter->Increment(rows_fetched_count);

  // rondis
  for (Uint32 i = 0; i < 61 /* not 64, rondis errors not counted */; i++) {
    Uint64 count = rondis_histogram[i].exchange(0, std::memory_order_relaxed);
    hist_counters[i] = count;
    // (i < 61) is always true
    hist_counters_dbl[i] = (double)count;
  }
  tot_count = 0;
  for (Uint32 i = 0; i < 61; i++) {
    tot_count += hist_counters[i];
  }
  if (tot_count > 0) {
    rondisCmdCounter->Increment(tot_count);
  }
  tot_value = rondis_histogram_total.exchange(0, std::memory_order_relaxed);
  rondisHistogram->ObserveMultiple(hist_counters_dbl,
                                   (double)tot_value / (double)1000000);

  // NDB
  // ndbKeyRequestFromPkReadCounter and ndbKeyRequestFromFsCounter already
  // incremented above.
  Uint64 count = m_ndb_key_request_from_batch_counter.exchange(
    0,
    std::memory_order_relaxed);
  ndbKeyRequestFromBatchCounter->Increment(count);
  count = m_ndb_key_request_from_fs_batch_counter.exchange(
    0,
    std::memory_order_relaxed);
  ndbKeyRequestFromBatchFsCounter->Increment(count);

  // ping
  count = m_ping_request_counter.exchange(0, std::memory_order_relaxed);
  pingCounter->Increment(count);

  // health
  count = m_health_request_counter.exchange(0, std::memory_order_relaxed);
  healthCounter->Increment(count);

  // metrics
  count = m_metrics_request_counter.exchange(0, std::memory_order_relaxed);
  metricsCounter->Increment(count);

  // Update RonDB Metrics, including
  // - ndbObjectsTotalCountGauge
  // - ronDBConnectionStateGauge
  setRonDBStats();

  // Update HTTP connection count from Drogon
  httpConnectionCountGauge->Set(
    static_cast<double>(drogon::app().getConnectionCount()));

  prometheus::TextSerializer serializer;
  std::ostringstream os;
  serializer.Serialize(os, registry->Collect());

  // Create an HTTP response with the serialized metrics
  resp->setBody(os.str());
  resp->setContentTypeString("text/plain; version=0.0.4");
  resp->setStatusCode(drogon::HttpStatusCode::k200OK);
}

}  // namespace rdrs_metrics
