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

#include "feature_store_scan_ctrl.hpp"
#include "api_key.hpp"
#include "batch_feature_store_ctrl.hpp"
#include "buffer_chain.hpp"
#include "buffer_manager.hpp"
#include "config_structs.hpp"
#include "constants.hpp"
#include "encoding.hpp"
#include "feature_store_ctrl.hpp"
#include "feature_store_data_structs.hpp"
#include "feature_util.hpp"
#include "fs_cache.hpp"
#include "json_parser.hpp"
#include "metadata.hpp"
#include "metrics.hpp"
#include "rate_limit.hpp"
#include "rdrs_dal.h"
#include "scan_metrics.hpp"
#include "scan_row_sink.hpp"
#include "status.hpp"

#include <ArenaMalloc.hpp>
#include <EventLogger.hpp>
#include <NdbTick.h>
#include <drogon/HttpTypes.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <simdjson.h>
#include <util/require.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_FSS_CTRL 1
#endif

#ifdef DEBUG_FSS_CTRL
#define DEB_FSS_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_FSS_CTRL(...) do { } while (0)
#endif

namespace fsds = feature_store_data_structs;

namespace feature_store_scan {

std::vector<char> FilterValueToJson(const Node::ParsedValue &value) {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  switch (value.kind) {
    case Node::ParsedValue::Kind::INT64:
      writer.Int64(value.i64);
      break;
    case Node::ParsedValue::Kind::UINT64:
      writer.Uint64(value.u64);
      break;
    case Node::ParsedValue::Kind::DOUBLE:
      writer.Double(value.d);
      break;
    case Node::ParsedValue::Kind::STRING:
      writer.String(value.s.data(),
                    static_cast<rapidjson::SizeType>(value.s.size()));
      break;
    default:
      writer.Null();
      break;
  }
  return std::vector<char>(buffer.GetString(),
                           buffer.GetString() + buffer.GetSize());
}

/*
 * Canonical text of a primary-key value for branch deduplication. RonDB
 * coerces the value to the column type, so 42, 42.0 and "42" all select
 * the same row and must collapse to one branch (D8); the JSON spelling
 * sent to the batch read is left as the client wrote it.
 */
static std::string dedupText(const Node::ParsedValue &value) {
  switch (value.kind) {
    case Node::ParsedValue::Kind::INT64:
      return std::to_string(value.i64);
    case Node::ParsedValue::Kind::UINT64:
      return std::to_string(value.u64);
    case Node::ParsedValue::Kind::DOUBLE: {
      const double d = value.d;
      if (std::trunc(d) == d &&
          d >= -9223372036854775808.0 && d < 9223372036854775808.0) {
        return std::to_string(static_cast<int64_t>(d));
      }
      char buf[32];
      snprintf(buf, sizeof(buf), "%.17g", d);
      return buf;
    }
    case Node::ParsedValue::Kind::STRING:
      return value.s;
    default:
      return "null";
  }
}

/*
 * Rewrite a column name as exposed by the feature view (prefix + feature
 * name) to the physical column of the root feature group's online table.
 */
static std::shared_ptr<RestErrorCode>
resolveColumn(std::string &column, const metadata::FeatureViewMetadata &md) {
  const metadata::FeatureGroupFeatures &root =
    md.featureGroupFeatures[md.rootFgIndex];
  auto features = md.prefixFeaturesLookup.find(column);
  if (features != md.prefixFeaturesLookup.end()) {
    for (const metadata::FeatureMetadata &f : features->second) {
      if (f.joinIndex == root.joinIndex &&
          f.featureGroupId == root.featureGroupId) {
        if (f.isComplex()) {
          return FILTER_ON_COMPLEX_FEATURE->NewMessage("'" + column + "'");
        }
        column = f.name;
        return nullptr;
      }
    }
  }
  for (const ServingKey &sk : root.primaryKeyMap) {
    for (const std::string &name : metadata::ServingKeyEntryNames(sk)) {
      if (name == column) {
        column = sk.featureName;
        return nullptr;
      }
    }
  }
  std::string msg = "'" + column + "'";
  msg += (features != md.prefixFeaturesLookup.end())
    ? " belongs to a joined feature group"
    : " is not a feature of the feature view";
  return FEATURE_NOT_IN_ROOT_FG->NewMessage(msg);
}

static std::shared_ptr<RestErrorCode>
resolveFilterColumns(const std::shared_ptr<FilterNode> &node,
                     const metadata::FeatureViewMetadata &md) {
  if (node == nullptr) {
    return nullptr;
  }
  if (node->type != FilterNode::Type::LOGIC) {
    return resolveColumn(node->column, md);
  }
  for (const auto &child : node->children) {
    auto err = resolveFilterColumns(child, md);
    if (err != nullptr) {
      return err;
    }
  }
  return nullptr;
}

std::shared_ptr<RestErrorCode>
ResolveScanColumns(ScanReadParams &scan,
                   const metadata::FeatureViewMetadata &md) {
  auto err = resolveFilterColumns(scan.filterRoot, md);
  if (err != nullptr) {
    return err;
  }
  if (scan.index != std::nullopt) {
    for (std::string &column : scan.index->columns) {
      err = resolveColumn(column, md);
      if (err != nullptr) {
        return err;
      }
    }
  }
  return nullptr;
}

/* Is this node an equality on a root primary-key column with a usable
 * value? On success `column` is the physical column. */
static bool isPkEquality(const FilterNode &node,
                         const std::unordered_set<std::string> &pkColumns,
                         std::string &column) {
  if (node.type != FilterNode::Type::COMPARE ||
      node.cond != NdbScanFilter::COND_EQ ||
      node.value.kind == Node::ParsedValue::Kind::NULLVAL ||
      node.value.kind == Node::ParsedValue::Kind::UNDEF) {
    return false;
  }
  if (pkColumns.count(node.column) == 0) {
    return false;
  }
  column = node.column;
  return true;
}

/* Serving keys of the root feature group, in serving-key order */
static const std::vector<ServingKey> &
rootServingKeys(const metadata::FeatureViewMetadata &md) {
  return md.featureGroupFeatures[md.rootFgIndex].primaryKeyMap;
}

static void addEntries(PkBranch &branch,
                       const metadata::FeatureViewMetadata &md,
                       const std::unordered_map<std::string,
                         Node::ParsedValue> &pkValues) {
  branch.dedupKey.clear();
  for (const ServingKey &sk : rootServingKeys(md)) {
    const Node::ParsedValue &value = pkValues.at(sk.featureName);
    branch.dedupKey += dedupText(value);
    branch.dedupKey.push_back('\x1f');
    std::vector<char> json = FilterValueToJson(value);
    for (const std::string &name : metadata::ServingKeyEntryNames(sk)) {
      branch.entries[name] = json;
    }
  }
}

/* The parsed filter is a binary tree (every logic node has exactly two
 * children), so "a AND b AND c" arrives as nested ANDs. Collect the leaves
 * of a same-group subtree; AND/OR are associative so this is lossless. */
static void flattenGroup(const std::shared_ptr<FilterNode> &node,
                         NdbScanFilter::Group group,
                         std::vector<std::shared_ptr<FilterNode>> &leaves) {
  if (node == nullptr) {
    return;
  }
  if (node->type == FilterNode::Type::LOGIC && node->group == group) {
    for (const auto &child : node->children) {
      flattenGroup(child, group, leaves);
    }
  } else {
    leaves.push_back(node);
  }
}

static bool collectBranch(const std::shared_ptr<FilterNode> &node,
                          const metadata::FeatureViewMetadata &md,
                          const std::unordered_set<std::string> &pkColumns,
                          PkBranch &branch) {
  std::unordered_map<std::string, Node::ParsedValue> pkValues;
  std::string column;
  if (node->type == FilterNode::Type::COMPARE) {
    if (pkColumns.size() != 1 || !isPkEquality(*node, pkColumns, column)) {
      return false;
    }
    pkValues[column] = node->value;
    branch.residual = nullptr;
  } else if (node->type == FilterNode::Type::LOGIC &&
             node->group == NdbScanFilter::AND) {
    std::vector<std::shared_ptr<FilterNode>> leaves;
    flattenGroup(node, NdbScanFilter::AND, leaves);
    std::vector<std::shared_ptr<FilterNode>> residualChildren;
    for (const auto &child : leaves) {
      if (isPkEquality(*child, pkColumns, column) &&
          pkValues.find(column) == pkValues.end()) {
        pkValues[column] = child->value;
      } else {
        residualChildren.push_back(child);
      }
    }
    if (pkValues.size() != pkColumns.size()) {
      return false;
    }
    if (residualChildren.empty()) {
      branch.residual = nullptr;
    } else if (residualChildren.size() == 1) {
      branch.residual = residualChildren[0];
    } else {
      auto residual = std::make_shared<FilterNode>();
      residual->type = FilterNode::Type::LOGIC;
      residual->group = NdbScanFilter::AND;
      residual->children = residualChildren;
      branch.residual = residual;
    }
  } else {
    return false;
  }
  addEntries(branch, md, pkValues);
  return true;
}

bool TryRewriteToPkBranches(const std::shared_ptr<FilterNode> &root,
                            const metadata::FeatureViewMetadata &md,
                            std::vector<PkBranch> &out) {
  out.clear();
  if (root == nullptr || md.rootFgIndex < 0 ||
      rootServingKeys(md).empty()) {
    return false;
  }
  std::unordered_set<std::string> pkColumns;
  for (const ServingKey &sk : rootServingKeys(md)) {
    pkColumns.insert(sk.featureName);
  }
  if (root->type == FilterNode::Type::LOGIC &&
      root->group == NdbScanFilter::OR) {
    std::vector<std::shared_ptr<FilterNode>> leaves;
    flattenGroup(root, NdbScanFilter::OR, leaves);
    if (leaves.empty()) {
      return false;
    }
    for (const auto &child : leaves) {
      PkBranch branch;
      if (!collectBranch(child, md, pkColumns, branch)) {
        out.clear();
        return false;
      }
      out.push_back(std::move(branch));
    }
    return true;
  }
  PkBranch branch;
  if (!collectBranch(root, md, pkColumns, branch)) {
    return false;
  }
  out.push_back(std::move(branch));
  return true;
}

bool DedupPkBranches(std::vector<PkBranch> &branches) {
  std::unordered_map<std::string, size_t> seen;
  std::vector<PkBranch> unique;
  unique.reserve(branches.size());
  for (auto &branch : branches) {
    auto it = seen.find(branch.dedupKey);
    if (it == seen.end()) {
      seen[branch.dedupKey] = unique.size();
      unique.push_back(std::move(branch));
      continue;
    }
    if (unique[it->second].residual != nullptr || branch.residual != nullptr) {
      /* Same key, different predicates: not a plain duplicate */
      return false;
    }
  }
  branches.swap(unique);
  return true;
}

bool IsTableScan(const ScanReadParams &scan) {
  if (scan.index == std::nullopt) {
    return true;
  }
  for (const IndexRange &range : scan.index->ranges) {
    if (range.lower == std::nullopt && range.upper == std::nullopt) {
      return true;
    }
  }
  return false;
}

bool TableScanAllowed(Internal::TableScanPolicy policy,
                      bool requestedByClient) {
  switch (policy) {
    case Internal::TableScanPolicy::ALLOW:
      return true;
    case Internal::TableScanPolicy::REQUEST:
      return requestedByClient;
    case Internal::TableScanPolicy::REJECT:
      return false;
  }
  return false;
}

}  // namespace feature_store_scan

using feature_store_scan::PkBranch;

namespace {

/* One assembled feature vector of the response */
struct RowResult {
  std::vector<std::vector<char>> features;
  fsds::FeatureStatus status = fsds::FeatureStatus::Complete;
  std::vector<fsds::DetailedStatus> detailedStatus;
};

/* A root row produced by the scan path */
struct RootRow {
  std::unordered_map<std::string, std::vector<char>> entries;
  /* (feature index in the vector, JSON text of the value) */
  std::vector<std::pair<int, std::vector<char>>> values;
  bool decodeError = false;
};

/* How each read column of the root scan is consumed */
struct ColumnPlan {
  std::string column;
  std::vector<std::string> entryNames;  // non-empty for pk columns
  int featureIndex = -1;                                   // fv features
  const metadata::AvroDecoder *decoder = nullptr;          // complex features
};

class RootRowSink : public ScanRowSink {
 public:
  RootRowSink(const std::vector<ColumnPlan> &plan, uint64_t limit)
    : m_plan(plan), m_limit(limit) {
  }
  void reset() override {
    m_rows.clear();
  }
  RS_Status on_row(
    const NdbRecord *table_rec,
    const char *row,
    const std::vector<const NdbDictionary::Column*> &read_columns) override {
    if (unlikely(read_columns.size() != m_plan.size())) {
      return RS_SERVER_ERROR("feature_store_scan: read column mismatch");
    }
    if (unlikely(m_rows.size() >= m_limit)) {
      return RS_OK;
    }
    RootRow out;
    std::vector<char> json;
    for (size_t i = 0; i < read_columns.size(); i++) {
      const ColumnPlan &plan = m_plan[i];
      const NdbDictionary::Column *column = read_columns[i];
      bool needJson = !plan.entryNames.empty() ||
                      (plan.featureIndex >= 0 && plan.decoder == nullptr);
      if (needJson) {
        ScanColumnToJson(table_rec, row, column, json);
      }
      for (const std::string &name : plan.entryNames) {
        out.entries[name] = json;
      }
      if (plan.featureIndex >= 0) {
        if (plan.decoder != nullptr) {
          std::vector<Uint8> raw;
          if (ScanColumnRawBytes(table_rec, row, column, raw)) {
            auto decoded = DeserialiseComplexFeature(raw, *plan.decoder);
            if (std::get<0>(decoded) != nullptr) {
              out.decodeError = true;
            } else {
              out.values.emplace_back(plan.featureIndex,
                                      std::get<1>(decoded));
              m_decodedComplex = true;
            }
          }
          /* NULL complex value: leave the slot empty (printed as null) */
        } else {
          out.values.emplace_back(plan.featureIndex, json);
        }
      }
    }
    m_rows.push_back(std::move(out));
    return RS_OK;
  }
  std::vector<RootRow> &rows() {
    return m_rows;
  }
  bool decodedComplex() const {
    return m_decodedComplex;
  }

 private:
  const std::vector<ColumnPlan> &m_plan;
  uint64_t m_limit;
  std::vector<RootRow> m_rows;
  bool m_decodedComplex = false;
};

/*
 * One pk_batch_read round-trip and its buffers. The parsed responses
 * point into the response chain, so the object must outlive any use of
 * `response`; the chains return their buffers to the pools when the
 * object is destroyed (RONDB-1144).
 */
class PkBatchRun {
 public:
  explicit PkBatchRun(size_t noOps)
    : m_noOps(noOps), m_amalloc(256 * 1024),
      m_reqChain(RS_BufferChain::REQ, static_cast<Uint32>(noOps)),
      m_respChain(RS_BufferChain::RESP, static_cast<Uint32>(noOps)) {
  }
  RS_Status run(std::vector<PKReadParams> &readParams,
                const std::vector<std::shared_ptr<FilterNode>> &opFilters,
                size_t threadIndex,
                const std::string &rl_identity) {
    require(readParams.size() == m_noOps);
    for (auto &readParam : readParams) {
      RS_Status status = readParam.validate();
      if (unlikely(status.http_code != SUCCESS)) {
        return status;
      }
    }
    RS_Status status = m_reqChain.take_first();
    if (likely(status.http_code == SUCCESS)) {
      status = m_respChain.take_first();
    }
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    for (Uint32 i = 0; i < m_noOps; i++) {
      if (i > 0) {
        status = m_reqChain.next_op_view(
          i, native_request_upper_bound(readParams[i]));
        if (unlikely(status.http_code != SUCCESS)) {
          return status;
        }
      }
      RS_Buffer &view = m_reqChain.views()[i];
      Uint32 consumed = 0;
      status = create_native_request(readParams[i], view, consumed);
      if (unlikely(status.http_code != SUCCESS)) {
        return status;
      }
      view.size = reinterpret_cast<Uint32 *>(view.buffer)[PK_REQ_LENGTH_IDX];
      m_reqChain.advance(consumed);
    }
    bool anyFilter = false;
    for (const auto &f : opFilters) {
      if (f != nullptr) {
        anyFilter = true;
        break;
      }
    }
    status = pk_batch_read(
      (void*)&m_amalloc,
      static_cast<unsigned int>(m_noOps),
      true,
      m_reqChain.views(),
      &m_respChain,
      static_cast<unsigned int>(threadIndex),
      rl_identity.empty() ? nullptr : rl_identity.c_str(),
      (unsigned int)rl_identity.size(),
      anyFilter ? static_cast<const void*>(opFilters.data()) : nullptr);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    response.Init(static_cast<int>(m_noOps));
    status = process_responses(&m_amalloc, m_respChain, m_reqChain.views(),
                               static_cast<Uint32>(m_noOps), response);
    if (unlikely(status.err_file_name[0] != '\0')) {
      return status;
    }
    return RS_OK;
  }
  BatchResponseJSON response;

 private:
  size_t m_noOps;
  ArenaMalloc m_amalloc;
  RS_BufferChain m_reqChain;
  RS_BufferChain m_respChain;
};

/* Group sub-responses "seq#opId" by seq and strip the prefix */
static RS_Status groupBySequence(
  BatchResponseJSON &batchResponse,
  size_t numEntries,
  std::vector<std::vector<PKReadResponseWithCodeJSON>> &grouped) {
  grouped.assign(numEntries, {});
  for (auto &response : batchResponse.getResult()) {
    std::string_view operationId = response.getBody().getOperationID();
    if (operationId.empty()) {
      continue;
    }
    size_t separatorPos = operationId.find(SEQUENCE_SEPARATOR);
    if (separatorPos == std::string_view::npos) {
      return RS_SERVER_ERROR("feature_store_scan: malformed operation id");
    }
    std::string_view seqStr = operationId.substr(0, separatorPos);
    std::string_view opId = operationId.substr(separatorPos + 1);
    size_t seqNum = 0;
    try {
      seqNum = std::stoul(std::string(seqStr));
    } catch (...) {
      return RS_SERVER_ERROR("feature_store_scan: malformed sequence number");
    }
    if (seqNum >= numEntries) {
      return RS_SERVER_ERROR("feature_store_scan: sequence out of range");
    }
    response.setOperationId(opId);
    grouped[seqNum].push_back(response);
  }
  return RS_OK;
}

static void sendError(const drogon::HttpResponsePtr &resp,
                      std::function<void(const drogon::HttpResponsePtr &)> &callback,
                      drogon::HttpStatusCode code,
                      const std::string &body) {
  resp->setBody(body);
  resp->setStatusCode(code);
  callback(resp);
}

static void sendRestError(const drogon::HttpResponsePtr &resp,
                          std::function<void(const drogon::HttpResponsePtr &)> &callback,
                          const std::shared_ptr<RestErrorCode> &err) {
  resp->setBody(err->Error());
  resp->setStatusCode(static_cast<drogon::HttpStatusCode>(err->GetStatus()));
  callback(resp);
}

/* Map a DAL status to the HTTP status of this endpoint (429 stays 429) */
static void sendDalError(const drogon::HttpResponsePtr &resp,
                         std::function<void(const drogon::HttpResponsePtr &)> &callback,
                         const RS_Status &status) {
  auto fsError = TranslateRonDbError(status.http_code, status.message);
  if (fsError == nullptr) {
    fsError = READ_FROM_DB_FAIL->NewMessage(status.message);
  }
  sendRestError(resp, callback, fsError);
}

static std::string buildResponseBody(
  const std::vector<RowResult> &rows,
  const metadata::FeatureViewMetadata &md,
  const fsds::MetadataRequest &metadataRequest,
  const fsds::ScanOptions &options,
  const std::string &warning) {
  std::string res = "{\"features\":[";
  for (size_t r = 0; r < rows.size(); r++) {
    if (r > 0) res += ",";
    res += "[";
    const auto &features = rows[r].features;
    for (size_t i = 0; i < features.size(); i++) {
      if (i > 0) res += ",";
      if (features[i].empty()) {
        res += "null";
      } else {
        res.append(features[i].begin(), features[i].end());
      }
    }
    res += "]";
  }
  res += "],\"metadata\":[";
  std::vector<fsds::FeatureMetadata> metadataArray;
  if (metadataRequest.featureName.value_or(false) ||
      metadataRequest.featureType.value_or(false)) {
    metadataArray = GetFeatureMetadata(&md, metadataRequest);
  }
  for (size_t i = 0; i < metadataArray.size(); i++) {
    if (i > 0) res += ",";
    res += "{\"featureName\":";
    if (metadataArray[i].name.empty()) {
      res += "null";
    } else {
      res += "\"" + metadataArray[i].name + "\"";
    }
    res += ",\"featureType\":";
    if (metadataArray[i].type.empty()) {
      res += "null";
    } else {
      res += "\"" + metadataArray[i].type + "\"";
    }
    res += "}";
  }
  res += "]";
  if (options.includeStatus) {
    res += ",\"status\":[";
    for (size_t r = 0; r < rows.size(); r++) {
      if (r > 0) res += ",";
      res += "\"" + fsds::toString(rows[r].status) + "\"";
    }
    res += "]";
  }
  if (options.includeDetailedStatus) {
    res += ",\"detailedStatus\":[";
    for (size_t r = 0; r < rows.size(); r++) {
      if (r > 0) res += ",";
      const auto &detailed = rows[r].detailedStatus;
      if (detailed.empty()) {
        res += "null";
        continue;
      }
      res += "[";
      for (size_t i = 0; i < detailed.size(); i++) {
        if (i > 0) res += ",";
        res += "{\"featureGroupId\":" +
          std::to_string(detailed[i].featureGroupId) +
          ",\"httpStatus\":" + std::to_string(detailed[i].httpStatus) + "}";
      }
      res += "]";
    }
    res += "]";
  }
  res += ",\"rows\":" + std::to_string(rows.size());
  if (!warning.empty()) {
    res += ",\"warning\":\"" + warning + "\"";
  }
  res += "}";
  return res;
}

/*
 * Fan-out for the scan path: primary-key reads to every non-root, non-spine
 * feature group for a chunk of root rows, then assembly of the vectors.
 * Root values come from the scan; joined values from the batch.
 */
static RS_Status assembleScanChunk(
  std::vector<RootRow> &rootRows,
  size_t begin,
  size_t end,
  const metadata::FeatureViewMetadata &md,
  const std::string &rootFgKey,
  bool includeDetailedStatus,
  size_t threadIndex,
  const std::string &rl_identity,
  bool &use_compressed,
  Uint32 &keyRequests,
  std::vector<RowResult> &out) {
  size_t numEntries = end - begin;
  std::vector<PKReadParams> readParams;
  for (size_t i = 0; i < numEntries; i++) {
    for (auto &param : GetBatchPkReadParams(md, rootRows[begin + i].entries)) {
      if (param.operationId == rootFgKey) {
        continue;  // root values already come from the scan
      }
      param.operationId =
        std::to_string(i) + SEQUENCE_SEPARATOR + param.operationId;
      readParams.push_back(param);
    }
  }
  std::vector<std::vector<PKReadResponseWithCodeJSON>> grouped(numEntries);
  std::unique_ptr<PkBatchRun> batch;
  if (!readParams.empty()) {
    batch = std::make_unique<PkBatchRun>(readParams.size());
    std::vector<std::shared_ptr<FilterNode>> noFilters(readParams.size());
    RS_Status status = batch->run(readParams, noFilters, threadIndex,
                                  rl_identity);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
    keyRequests += static_cast<Uint32>(readParams.size());
    status = groupBySequence(batch->response, numEntries, grouped);
    if (unlikely(status.http_code != SUCCESS)) {
      return status;
    }
  }
  for (size_t i = 0; i < numEntries; i++) {
    RootRow &rootRow = rootRows[begin + i];
    auto result = GetFeatureValues(grouped[i], rootRow.entries, md,
                                   includeDetailedStatus, use_compressed);
    RowResult row;
    row.features = std::move(std::get<0>(result));
    row.status = std::get<1>(result);
    row.detailedStatus = std::move(std::get<2>(result));
    if (row.features.size() != static_cast<size_t>(md.numOfFeatures)) {
      row.features.resize(md.numOfFeatures);
    }
    for (auto &value : rootRow.values) {
      if (value.first >= 0 &&
          static_cast<size_t>(value.first) < row.features.size()) {
        row.features[value.first] = std::move(value.second);
      }
    }
    if (rootRow.decodeError) {
      row.status = fsds::FeatureStatus::Error;
    }
    if (includeDetailedStatus) {
      size_t pos = 0;
      for (int k = 0; k < md.rootFgIndex; k++) {
        if (!md.featureGroupFeatures[k].isSpine()) {
          pos++;
        }
      }
      fsds::DetailedStatus rootStatus{};
      rootStatus.featureGroupId =
        md.featureGroupFeatures[md.rootFgIndex].featureGroupId;
      rootStatus.httpStatus = static_cast<Int32>(drogon::k200OK);
      pos = std::min(pos, row.detailedStatus.size());
      row.detailedStatus.insert(row.detailedStatus.begin() + pos, rootStatus);
    }
    out.push_back(std::move(row));
  }
  return RS_OK;
}

/*
 * Fast path: one chunk of primary-key branches, all feature groups read in
 * one batch, the residual predicate attached to the root read. Branches
 * whose root row is absent or rejected produce no output row.
 */
static RS_Status assemblePkChunk(
  std::vector<PkBranch> &branches,
  size_t begin,
  size_t end,
  const metadata::FeatureViewMetadata &md,
  const std::string &rootFgKey,
  bool includeDetailedStatus,
  size_t threadIndex,
  const std::string &rl_identity,
  uint64_t limit,
  bool &use_compressed,
  Uint32 &keyRequests,
  std::vector<RowResult> &out) {
  size_t numEntries = end - begin;
  std::vector<PKReadParams> readParams;
  std::vector<std::shared_ptr<FilterNode>> opFilters;
  for (size_t i = 0; i < numEntries; i++) {
    PkBranch &branch = branches[begin + i];
    for (auto &param : GetBatchPkReadParams(md, branch.entries)) {
      opFilters.push_back(param.operationId == rootFgKey
                          ? branch.residual : nullptr);
      param.operationId =
        std::to_string(i) + SEQUENCE_SEPARATOR + param.operationId;
      readParams.push_back(param);
    }
  }
  if (readParams.empty()) {
    return RS_OK;
  }
  PkBatchRun batch(readParams.size());
  RS_Status status = batch.run(readParams, opFilters, threadIndex,
                               rl_identity);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  keyRequests += static_cast<Uint32>(readParams.size());
  std::vector<std::vector<PKReadResponseWithCodeJSON>> grouped;
  status = groupBySequence(batch.response, numEntries, grouped);
  if (unlikely(status.http_code != SUCCESS)) {
    return status;
  }
  for (size_t i = 0; i < numEntries && out.size() < limit; i++) {
    bool rootAbsent = true;
    for (const auto &response : grouped[i]) {
      if (response.getBody().getOperationIdString() == rootFgKey) {
        rootAbsent =
          (response.getBody().getStatusCode() == drogon::k404NotFound);
        break;
      }
    }
    if (rootAbsent) {
      continue;
    }
    auto result = GetFeatureValues(grouped[i], branches[begin + i].entries,
                                   md, includeDetailedStatus, use_compressed);
    RowResult row;
    row.features = std::move(std::get<0>(result));
    row.status = std::get<1>(result);
    row.detailedStatus = std::move(std::get<2>(result));
    out.push_back(std::move(row));
  }
  return RS_OK;
}

}  // namespace

void FeatureStoreScanCtrl::featureStoreScan(
    const drogon::HttpRequestPtr &req,
    std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  /*
   * 1. Parse the JSON request
   * 2. Feature view metadata from the cache
   * 3. Root feature group / star schema checks, limit
   * 4. Resolve filter and index columns to the root online table
   * 5. Authenticate
   * 6. Primary-key fast path, or
   * 7. Scan the root table and fan out to the joined feature groups
   * 8. Serialise
   */
  bool use_compressed = globalConfigs.rest.useCompression;
  drogon::HttpResponsePtr resp = drogon::HttpResponse::newHttpResponse();
  FeatureStoreScanEndPointMetricsUpdater metricsUpdater(resp);

  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  if (unlikely(currentThreadIndex >= globalConfigs.rest.numThreads)) {
    sendError(resp, callback, drogon::k500InternalServerError,
              "Too many threads");
    return;
  }
  JSONParser &jsonParser = jsonParsers[currentThreadIndex];

  const char *json_str = req->getBody().data();
  DEB_FSS_CTRL("\n\n JSON REQUEST: \n %s \n", json_str);
  size_t length = req->getBody().length();
  if (unlikely(length > globalConfigs.internal.maxHttpReqSize)) {
    sendError(resp, callback, drogon::k400BadRequest, "Request too large");
    return;
  }
  memcpy(jsonParser.get_buffer().get(), json_str, length);

  fsds::FeatureStoreScanRequest reqStruct;
  RS_Status status = jsonParser.feature_store_scan_parse(
      simdjson::padded_string_view(jsonParser.get_buffer().get(), length,
                                   globalConfigs.internal.maxHttpReqSize +
                                   simdjson::SIMDJSON_PADDING),
      reqStruct);
  if (unlikely(status.http_code != SUCCESS)) {
    sendError(resp, callback, drogon::k400BadRequest,
              std::string("Error:") + status.message);
    return;
  }

  // 2. Metadata
  char *metadata_cache_entry = nullptr;
  auto [metadata, err] =
    metadata::FeatureViewMetadataCache_Get(
      reqStruct.featureStoreName,
      reqStruct.featureViewName,
      reqStruct.featureViewVersion,
      &metadata_cache_entry);
  CacheEntryRefCounter cache_entry_ref_counter(metadata_cache_entry);
  if (unlikely(err != nullptr)) {
    sendRestError(resp, callback, err);
    return;
  }

  // 3. Root feature group, star schema, limit
  if (unlikely(metadata->rootFgIndex < 0)) {
    sendRestError(resp, callback, ROOT_FG_NOT_ONLINE->NewMessage(
      "Feature view has no root feature group with online features"));
    return;
  }
  const metadata::FeatureGroupFeatures &rootFg =
    metadata->featureGroupFeatures[metadata->rootFgIndex];
  if (unlikely(rootFg.isSpine() || rootFg.onDemandFeatureGroupID != 0)) {
    sendRestError(resp, callback, ROOT_FG_NOT_ONLINE->NewMessage(
      "Feature group " + rootFg.featureGroupName + " is " +
      (rootFg.isSpine() ? "a spine" : "an on-demand") + " feature group"));
    return;
  }
  if (unlikely(!metadata->isStarSchema)) {
    sendRestError(resp, callback, FEATURE_VIEW_NOT_STAR_SCHEMA);
    return;
  }
  uint64_t maxLimit = globalConfigs.internal.featureStoreScanMaxLimit;
  uint64_t limit = reqStruct.limitProvided
    ? reqStruct.scan.limit
    : globalConfigs.internal.featureStoreScanDefaultLimit;
  if (unlikely(limit > maxLimit)) {
    sendRestError(resp, callback, LIMIT_EXCEEDED->NewMessage(
      "limit " + std::to_string(limit) + " exceeds FeatureStoreScanMaxLimit " +
      std::to_string(maxLimit)));
    return;
  }
  reqStruct.scan.limit = limit;
  const std::string rootFgKey = metadata::GetFeatureGroupKeyByTDFeature(rootFg);

  // 4. Column resolution and scan validation
  err = feature_store_scan::ResolveScanColumns(reqStruct.scan, *metadata);
  if (unlikely(err != nullptr)) {
    sendRestError(resp, callback, err);
    return;
  }
  if (reqStruct.scan.filterRoot) {
    status = ValidateScanFilter(reqStruct.scan.filterRoot);
    if (unlikely(status.http_code != SUCCESS)) {
      sendError(resp, callback, drogon::k400BadRequest, status.message);
      return;
    }
  }
  if (reqStruct.scan.index != std::nullopt) {
    status = ValidateScanIndex(reqStruct.scan.index.value());
    if (unlikely(status.http_code != SUCCESS)) {
      sendError(resp, callback, drogon::k400BadRequest, status.message);
      return;
    }
  }

  // 5. Authenticate
  std::string rl_identity;
  if (likely(globalConfigs.security.apiKey.useHopsworksAPIKeys)) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status = authenticate(api_key, *metadata);
    if (unlikely(status.http_code != SUCCESS)) {
      sendError(resp, callback,
                static_cast<drogon::HttpStatusCode>(status.http_code),
                status.message);
      return;
    }
    rl_identity = get_rate_limit_identity(api_key);
  }

  fsds::ScanOptions options = reqStruct.GetOptions();
  std::vector<RowResult> rows;
  std::string warning;
  Uint32 keyRequests = 0;
  const size_t chunkSize =
    std::max<Uint32>(1, globalConfigs.internal.featureStoreScanBatchSize);

  // 6. Primary-key fast path
  std::vector<PkBranch> branches;
  bool fastPath = (reqStruct.scan.index == std::nullopt) &&
    feature_store_scan::TryRewriteToPkBranches(reqStruct.scan.filterRoot,
                                               *metadata, branches) &&
    feature_store_scan::DedupPkBranches(branches);
  if (fastPath) {
    metricsUpdater.set_fast_path(true);
    if (unlikely(branches.size() > maxLimit)) {
      sendRestError(resp, callback, LIMIT_EXCEEDED->NewMessage(
        std::to_string(branches.size()) +
        " primary-key branches exceed FeatureStoreScanMaxLimit " +
        std::to_string(maxLimit)));
      return;
    }
    for (size_t begin = 0; begin < branches.size() && rows.size() < limit;
         begin += chunkSize) {
      size_t end = std::min(branches.size(), begin + chunkSize);
      status = assemblePkChunk(branches, begin, end, *metadata, rootFgKey,
                               options.includeDetailedStatus,
                               currentThreadIndex, rl_identity, limit,
                               use_compressed, keyRequests, rows);
      if (unlikely(status.http_code != SUCCESS)) {
        sendDalError(resp, callback, status);
        return;
      }
    }
  } else {
    // 7. Scan path
    if (feature_store_scan::IsTableScan(reqStruct.scan)) {
      const Internal::TableScanPolicy policy =
        globalConfigs.internal.tableScanPolicy();
      if (!feature_store_scan::TableScanAllowed(policy,
                                                options.allowTableScan)) {
        std::string hint = "Filter on the root feature group's primary key "
                           "or give a bounded index range";
        if (policy == Internal::TableScanPolicy::REQUEST) {
          hint += ", or set options.allowTableScan to true";
        } else {
          hint += "; the server's FeatureStoreScanTableScanPolicy is reject";
        }
        sendRestError(resp, callback,
                      TABLE_SCAN_NOT_ALLOWED->NewMessage(hint));
        return;
      }
      warning = "full scan of root feature group; consider filtering on the "
                "primary key or adding a bounded index range";
    }
    reqStruct.scan.path.db = rootFg.featureStoreName;
    reqStruct.scan.path.table = rootFg.featureGroupName + "_" +
      std::to_string(rootFg.featureGroupVersion);

    /* Read columns: every root feature plus every root primary-key column */
    std::vector<ColumnPlan> plan;
    std::unordered_map<std::string, size_t> planIndex;
    auto planFor = [&](const std::string &column) -> ColumnPlan& {
      auto it = planIndex.find(column);
      if (it != planIndex.end()) {
        return plan[it->second];
      }
      planIndex[column] = plan.size();
      plan.emplace_back();
      plan.back().column = column;
      return plan.back();
    };
    for (const ServingKey &sk : rootFg.primaryKeyMap) {
      planFor(sk.featureName).entryNames = metadata::ServingKeyEntryNames(sk);
    }
    for (const metadata::FeatureMetadata &feature : rootFg.features) {
      ColumnPlan &cp = planFor(feature.name);
      std::string featureIndexKey = metadata::GetFeatureIndexKeyByFeature(feature);
      auto idx = metadata->featureIndexLookup.find(featureIndexKey);
      if (idx != metadata->featureIndexLookup.end()) {
        cp.featureIndex = idx->second;
      }
      auto decoder = metadata->complexFeatures.find(featureIndexKey);
      if (decoder != metadata->complexFeatures.end()) {
        cp.decoder = &decoder->second;
      }
    }
    reqStruct.scan.readColumns.clear();
    for (const ColumnPlan &cp : plan) {
      ScanReadColumn rc;
      rc.column = cp.column;  // plan outlives the scan
      reqStruct.scan.readColumns.push_back(rc);
    }

    bool timing_enabled = g_scan_timing_enabled;
    ScanPhaseTiming timing;
    NDB_TICKS total_start;
    if (timing_enabled) {
      total_start = NdbTick_getCurrentTicks();
    }
    RootRowSink sink(plan, limit);
    uint64_t rows_fetched = 0;
    status = scan_read_rows(reqStruct.scan, currentThreadIndex, &sink,
                            rl_identity.empty() ? nullptr : rl_identity.c_str(),
                            (unsigned int)rl_identity.size(),
                            &rows_fetched, timing_enabled ? &timing : nullptr);
    if (unlikely(status.http_code != SUCCESS)) {
      if (status.http_code == TOO_MANY_REQUESTS) {
        sendRestError(resp, callback,
                      RATE_LIMIT_EXCEEDED->NewMessage(status.message));
      } else if (status.http_code == SERVER_ERROR) {
        sendRestError(resp, callback,
                      READ_FROM_DB_FAIL->NewMessage(status.message));
      } else {
        sendRestError(resp, callback,
                      READ_FROM_DB_FAIL_BAD_INPUT->NewMessage(status.message));
      }
      return;
    }
    if (timing_enabled) {
      timing.total_us =
        NdbTick_Elapsed(total_start, NdbTick_getCurrentTicks()).microSec();
      timing.database = reqStruct.scan.path.db;
      timing.table = reqStruct.scan.path.table;
      timing.limit = limit;
      timing.has_filter = (reqStruct.scan.filterRoot != nullptr);
      timing.is_index_scan = (reqStruct.scan.index != std::nullopt);
      if (timing.is_index_scan) {
        timing.index_name = reqStruct.scan.index.value().name;
      }
      maybeRecordSlowScan(timing, currentThreadIndex);
    }
    if (sink.decodedComplex()) {
      use_compressed = false;
    }
    std::vector<RootRow> &rootRows = sink.rows();
    for (size_t begin = 0; begin < rootRows.size(); begin += chunkSize) {
      size_t end = std::min(rootRows.size(), begin + chunkSize);
      status = assembleScanChunk(rootRows, begin, end, *metadata, rootFgKey,
                                 options.includeDetailedStatus,
                                 currentThreadIndex, rl_identity,
                                 use_compressed, keyRequests, rows);
      if (unlikely(status.http_code != SUCCESS)) {
        sendDalError(resp, callback, status);
        return;
      }
    }
  }

  /* Spine feature groups are external: as in /batch_feature_store every
   * row of a view with a spine FG reports MISSING unless in error. */
  if (metadata->hasSpine) {
    for (auto &row : rows) {
      if (row.status != fsds::FeatureStatus::Error) {
        row.status = fsds::FeatureStatus::Missing;
      }
    }
  }

  // 8. Serialise
  metricsUpdater.set_rows_fetched(rows.size());
  metricsUpdater.set_key_requests(keyRequests);
  if (use_compressed) {
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
  } else {
    resp->setContentTypeCode(drogon::CT_APPLICATION_OCTET_STREAM);
  }
  std::string body = buildResponseBody(rows, *metadata,
                                       reqStruct.metadataRequest,
                                       options, warning);
  DEB_FSS_CTRL("JSON response: %s", body.c_str());
  resp->setBody(std::move(body));
  resp->setStatusCode(drogon::k200OK);
  callback(resp);
}
