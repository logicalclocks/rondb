/*
 * Copyright (c) 2026, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_DATA_STRUCTS_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_DATA_STRUCTS_HPP_

#include "feature_store_data_structs.hpp"
#include "pk_data_structs.hpp"

#include <optional>
#include <string>

namespace feature_store_data_structs {

class ScanOptionsRequest {
 public:
  std::optional<bool> includeDetailedStatus;  // json:"includeDetailedStatus"
  std::optional<bool> includeStatus;          // json:"includeStatus"
  std::optional<bool> allowTableScan;         // json:"allowTableScan"
};

class ScanOptions {
 public:
  bool includeDetailedStatus = false;
  bool includeStatus = true;
  bool allowTableScan = false;
};

/*
 * Request of feature vectors of a feature view selected by a scan of the
 * root feature group. The scan sub-request (filters, index, limit) uses the
 * /scan grammar; its column names are feature-view names and are resolved
 * to the root feature group's online columns by the controller.
 */
class FeatureStoreScanRequest {
 public:
  FeatureStoreScanRequest() : featureViewVersion(0), scan("", "") {
  }
  std::string featureStoreName;
  std::string featureViewName;
  int featureViewVersion;
  ScanReadParams scan;
  bool limitProvided = false;
  MetadataRequest metadataRequest;
  ScanOptionsRequest optionsRequest;

  ScanOptions GetOptions() const {
    ScanOptions options;
    options.includeDetailedStatus =
      optionsRequest.includeDetailedStatus.value_or(false);
    options.includeStatus =
      optionsRequest.includeStatus.value_or(true) ||
      options.includeDetailedStatus;
    options.allowTableScan = optionsRequest.allowTableScan.value_or(false);
    return options;
  }
};

}  // namespace feature_store_data_structs

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_DATA_STRUCTS_HPP_
