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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_CTRL_HPP_

#include "config_structs.hpp"
#include "constants.hpp"
#include "feature_store_error_code.hpp"
#include "feature_store_scan_data_structs.hpp"
#include "metadata.hpp"
#include "pk_data_structs.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

/*
 * POST /0.1.0/feature_store_scan
 *
 * Feature vectors of a feature view selected by a scan of the view's root
 * feature group (filters / index range / limit, as /scan), joined to the
 * other feature groups by primary-key reads, with complex features
 * Avro-decoded. Requires a star-schema feature view. When the filter is a
 * full-primary-key equality, the request is served by the /batch_feature_store
 * machinery, with the residual predicate pushed into the root primary-key
 * read as an interpreted program.
 */
class FeatureStoreScanCtrl
    : public drogon::HttpController<FeatureStoreScanCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(FeatureStoreScanCtrl::featureStoreScan,
                FEATURE_STORE_SCAN_PATH,
                drogon::Post);
  METHOD_LIST_END

  static void
  featureStoreScan(
      const drogon::HttpRequestPtr &req,
      std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

namespace feature_store_scan {

/*
 * One primary-key branch of a fast-path request: the serving-key entries
 * that identify the root row (in the form /batch_feature_store accepts),
 * a key used to deduplicate branches, and the part of the predicate that
 * is not a primary-key equality (nullptr when there is none).
 */
struct PkBranch {
  std::unordered_map<std::string, std::vector<char>> entries;
  std::string dedupKey;
  std::shared_ptr<FilterNode> residual;
};

/*
 * Rewrite filter column names (feature names as exposed by the feature
 * view) to the physical online columns of the root feature group.
 */
std::shared_ptr<RestErrorCode>
ResolveScanColumns(ScanReadParams &scan,
                   const metadata::FeatureViewMetadata &md);

/*
 * Decide whether a (column-resolved) filter tree can be served by
 * primary-key reads instead of a scan, and if so produce its branches.
 */
bool
TryRewriteToPkBranches(const std::shared_ptr<FilterNode> &root,
                       const metadata::FeatureViewMetadata &md,
                       std::vector<PkBranch> &out);

/*
 * Drop branches that repeat an earlier branch's key. Returns false if a
 * repeated key has a residual predicate (the residuals would need merging);
 * the caller then uses the scan path.
 */
bool
DedupPkBranches(std::vector<PkBranch> &branches);

std::vector<char>
FilterValueToJson(const Node::ParsedValue &value);

bool
IsTableScan(const ScanReadParams &scan);

bool
TableScanAllowed(Internal::TableScanPolicy policy, bool requestedByClient);

}  // namespace feature_store_scan

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_SCAN_CTRL_HPP_
