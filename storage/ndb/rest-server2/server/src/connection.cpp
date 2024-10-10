/*
 * Copyright (C) 2023, 2024 Hopsworks AB
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

#include "connection.hpp"
#include "config_structs.hpp"
#include "rdrs_dal.h"
#include "rdrs_dal.hpp"

#include <drogon/HttpTypes.h>

RS_Status RonDBConnection::init_rondb_connection(RonDB &rondbDataCluster,
                                                 RonDB &rondbMetaDataCluster,
                                                 Uint32 numThreads) noexcept {
  // init RonDB client API

  RS_Status ret = init(numThreads, rondbDataCluster.connectionPoolSize);
  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }

  // Connect to data cluster
  std::string csd = rondbDataCluster.generate_Mgmd_connect_string();

  std::unique_ptr<unsigned int[]> dataClusterNodeIDsMem(
      new unsigned int[rondbDataCluster.nodeIDs.size()]);

  for (size_t i = 0; i < rondbDataCluster.nodeIDs.size(); ++i) {
    dataClusterNodeIDsMem[i] =
      static_cast<unsigned int>(rondbDataCluster.nodeIDs[i]);
  }

  ret = add_data_connection(csd.c_str(),
                            rondbDataCluster.connectionPoolSize,
                            dataClusterNodeIDsMem.get(),
                            rondbDataCluster.nodeIDs.size(),
                            rondbDataCluster.connectionRetries,
                            rondbDataCluster.connectionRetryDelayInSec);

  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }

  ret = set_data_cluster_op_retry_props(
    rondbDataCluster.opRetryOnTransientErrorsCount,
    rondbDataCluster.opRetryInitialDelayInMS,
    rondbDataCluster.opRetryJitterInMS);
  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }
  // Connect to metadata cluster
  std::string csmd = rondbMetaDataCluster.generate_Mgmd_connect_string();
  std::unique_ptr<unsigned int[]> metaClusterNodeIDsMem(
      new unsigned int[rondbMetaDataCluster.nodeIDs.size()]);
  for (size_t i = 0; i < rondbMetaDataCluster.nodeIDs.size(); ++i) {
    metaClusterNodeIDsMem[i] =
      static_cast<unsigned int>(rondbMetaDataCluster.nodeIDs[i]);
  }
  ret = add_metadata_connection(csmd.c_str(),
                                rondbMetaDataCluster.connectionPoolSize,
                                metaClusterNodeIDsMem.get(),
                                rondbMetaDataCluster.nodeIDs.size(),
                                rondbMetaDataCluster.connectionRetries,
                                rondbMetaDataCluster.connectionRetryDelayInSec);

  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }
  ret = set_metadata_cluster_op_retry_props(
    rondbMetaDataCluster.opRetryOnTransientErrorsCount,
    rondbMetaDataCluster.opRetryInitialDelayInMS,
    rondbMetaDataCluster.opRetryJitterInMS);

  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status RonDBConnection::shutdown_rondb_connection() noexcept {
  RS_Status ret = shutdown_connection();
  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }
  return CRS_Status::SUCCESS.status;
}

RS_Status RonDBConnection::rondb_reconnect() noexcept {
  RS_Status ret = reconnect();
  if (static_cast<drogon::HttpStatusCode>(ret.http_code) !=
        drogon::HttpStatusCode::k200OK) {
    return ret;
  }
  return CRS_Status::SUCCESS.status;
}
