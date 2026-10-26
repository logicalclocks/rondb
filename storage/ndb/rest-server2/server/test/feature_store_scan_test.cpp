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

/*
 * Unit tests for the feature_store_scan fast-path detection and column
 * resolution. They run on a hand-built FeatureViewMetadata and need no
 * cluster; the end-to-end behaviour is covered by the Go integration tests
 * in test_go/internal/integrationtests/featurestorescan.
 */

#include "feature_store_scan_ctrl.hpp"
#include "feature_store_error_code.hpp"
#include "metadata.hpp"
#include "pk_data_structs.hpp"

#include <NdbMutex.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

/* Defined in main.cc, which the test library deliberately excludes */
NdbMutex *globalConfigsMutex = nullptr;

using feature_store_scan::PkBranch;
using feature_store_scan::DedupPkBranches;
using feature_store_scan::FilterValueToJson;
using feature_store_scan::ResolveScanColumns;
using feature_store_scan::TryRewriteToPkBranches;

namespace {

std::shared_ptr<FilterNode> cmp(const std::string &column,
                                NdbScanFilter::BinaryCondition cond,
                                int64_t value) {
  auto node = std::make_shared<FilterNode>();
  node->type = FilterNode::Type::COMPARE;
  node->cond = cond;
  node->column = column;
  node->value.kind = Node::ParsedValue::Kind::INT64;
  node->value.i64 = value;
  return node;
}

std::shared_ptr<FilterNode> cmpStr(const std::string &column,
                                   const std::string &value) {
  auto node = std::make_shared<FilterNode>();
  node->type = FilterNode::Type::COMPARE;
  node->cond = NdbScanFilter::COND_EQ;
  node->column = column;
  node->value.kind = Node::ParsedValue::Kind::STRING;
  node->value.s = value;
  return node;
}

std::shared_ptr<FilterNode> cmpDbl(const std::string &column, double value) {
  auto node = std::make_shared<FilterNode>();
  node->type = FilterNode::Type::COMPARE;
  node->cond = NdbScanFilter::COND_EQ;
  node->column = column;
  node->value.kind = Node::ParsedValue::Kind::DOUBLE;
  node->value.d = value;
  return node;
}

std::shared_ptr<FilterNode> isNotNull(const std::string &column) {
  auto node = std::make_shared<FilterNode>();
  node->type = FilterNode::Type::IS_NOT_NULL;
  node->column = column;
  return node;
}

std::shared_ptr<FilterNode> logic(NdbScanFilter::Group group,
                                  std::shared_ptr<FilterNode> a,
                                  std::shared_ptr<FilterNode> b) {
  auto node = std::make_shared<FilterNode>();
  node->type = FilterNode::Type::LOGIC;
  node->group = group;
  node->children = {a, b};
  return node;
}

/*
 * Feature view with root FG (joinIndex 0, id 1): pk columns `pkColumns`,
 * feature "data" exposed with prefix "root_", complex feature "tags"; and a
 * joined FG (joinIndex 1, id 2) exposing "fg2_data".
 */
metadata::FeatureViewMetadata makeMetadata(
  const std::vector<std::string> &pkColumns) {
  metadata::FeatureViewMetadata md;
  md.rootFgIndex = 0;
  md.isStarSchema = true;
  md.featureGroupFeatures.emplace_back();
  metadata::FeatureGroupFeatures &root = md.featureGroupFeatures[0];
  root.joinIndex = 0;
  root.featureGroupId = 1;
  auto feature = [](int joinIndex, int fgId, const std::string &name,
                    const std::string &prefix, const std::string &type) {
    metadata::FeatureMetadata f;
    f.joinIndex = joinIndex;
    f.featureGroupId = fgId;
    f.name = name;
    f.prefix = prefix;
    f.type = type;
    return f;
  };
  for (const auto &pk : pkColumns) {
    ServingKey sk;
    sk.featureName = pk;
    root.primaryKeyMap.push_back(sk);
    md.prefixFeaturesLookup[pk] = {feature(0, 1, pk, "", "bigint")};
  }
  md.prefixFeaturesLookup["root_data"] = {feature(0, 1, "data", "root_", "int")};
  md.prefixFeaturesLookup["tags"] = {feature(0, 1, "tags", "", "array<string>")};
  md.prefixFeaturesLookup["fg2_data"] = {feature(1, 2, "data", "fg2_", "int")};
  return md;
}

std::string str(const std::vector<char> &v) {
  return std::string(v.begin(), v.end());
}

}  // namespace

TEST(FeatureStoreScanFastPath, SinglePkEquality) {
  auto md = makeMetadata({"id"});
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(cmp("id", NdbScanFilter::COND_EQ, 42),
                                     md, branches));
  ASSERT_EQ(branches.size(), 1u);
  EXPECT_EQ(branches[0].residual, nullptr);
  ASSERT_EQ(branches[0].entries.count("id"), 1u);
  EXPECT_EQ(str(branches[0].entries["id"]), "42");
  EXPECT_FALSE(branches[0].dedupKey.empty());
}

TEST(FeatureStoreScanFastPath, NonEqualityOnPkIsNotFastPath) {
  auto md = makeMetadata({"id"});
  std::vector<PkBranch> branches;
  EXPECT_FALSE(TryRewriteToPkBranches(cmp("id", NdbScanFilter::COND_GE, 42),
                                      md, branches));
  EXPECT_FALSE(TryRewriteToPkBranches(isNotNull("id"), md, branches));
  EXPECT_FALSE(TryRewriteToPkBranches(cmp("data", NdbScanFilter::COND_EQ, 1),
                                      md, branches));
  EXPECT_FALSE(TryRewriteToPkBranches(nullptr, md, branches));
}

TEST(FeatureStoreScanFastPath, NullValueIsNotFastPath) {
  auto md = makeMetadata({"id"});
  auto node = cmp("id", NdbScanFilter::COND_EQ, 0);
  node->value.kind = Node::ParsedValue::Kind::NULLVAL;
  std::vector<PkBranch> branches;
  EXPECT_FALSE(TryRewriteToPkBranches(node, md, branches));
}

TEST(FeatureStoreScanFastPath, AndWithResidual) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::AND,
                    cmp("data", NdbScanFilter::COND_GE, 5),
                    cmp("id", NdbScanFilter::COND_EQ, 7));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 1u);
  EXPECT_EQ(str(branches[0].entries["id"]), "7");
  ASSERT_NE(branches[0].residual, nullptr);
  EXPECT_EQ(branches[0].residual->type, FilterNode::Type::COMPARE);
  EXPECT_EQ(branches[0].residual->column, "data");
}

TEST(FeatureStoreScanFastPath, NestedAndIsFlattened) {
  auto md = makeMetadata({"id1", "id2"});
  /* AND(AND(id1 = 1, id2 = "x"), AND(data >= 5, data IS NOT NULL)) */
  auto root = logic(NdbScanFilter::AND,
                    logic(NdbScanFilter::AND,
                          cmp("id1", NdbScanFilter::COND_EQ, 1),
                          cmpStr("id2", "x")),
                    logic(NdbScanFilter::AND,
                          cmp("data", NdbScanFilter::COND_GE, 5),
                          isNotNull("data")));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 1u);
  EXPECT_EQ(str(branches[0].entries["id1"]), "1");
  EXPECT_EQ(str(branches[0].entries["id2"]), "\"x\"");
  ASSERT_NE(branches[0].residual, nullptr);
  EXPECT_EQ(branches[0].residual->type, FilterNode::Type::LOGIC);
  EXPECT_EQ(branches[0].residual->group, NdbScanFilter::AND);
  EXPECT_EQ(branches[0].residual->children.size(), 2u);
}

TEST(FeatureStoreScanFastPath, PartialCompositeKeyIsNotFastPath) {
  auto md = makeMetadata({"id1", "id2"});
  std::vector<PkBranch> branches;
  EXPECT_FALSE(TryRewriteToPkBranches(cmp("id1", NdbScanFilter::COND_EQ, 1),
                                      md, branches));
  auto root = logic(NdbScanFilter::AND,
                    cmp("id1", NdbScanFilter::COND_EQ, 1),
                    cmp("data", NdbScanFilter::COND_EQ, 1));
  EXPECT_FALSE(TryRewriteToPkBranches(root, md, branches));
}

TEST(FeatureStoreScanFastPath, RepeatedPkEqualityBecomesResidual) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::AND,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    cmp("id", NdbScanFilter::COND_EQ, 2));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 1u);
  EXPECT_EQ(str(branches[0].entries["id"]), "1");
  ASSERT_NE(branches[0].residual, nullptr);
  EXPECT_EQ(str(FilterValueToJson(branches[0].residual->value)), "2");
}

TEST(FeatureStoreScanFastPath, NestedOrGivesOneBranchPerLeaf) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::OR,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    logic(NdbScanFilter::OR,
                          logic(NdbScanFilter::AND,
                                cmp("id", NdbScanFilter::COND_EQ, 2),
                                cmp("data", NdbScanFilter::COND_LT, 9)),
                          cmp("id", NdbScanFilter::COND_EQ, 3)));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 3u);
  EXPECT_EQ(str(branches[0].entries["id"]), "1");
  EXPECT_EQ(str(branches[1].entries["id"]), "2");
  EXPECT_NE(branches[1].residual, nullptr);
  EXPECT_EQ(str(branches[2].entries["id"]), "3");
  EXPECT_EQ(branches[2].residual, nullptr);
  EXPECT_NE(branches[0].dedupKey, branches[1].dedupKey);
}

TEST(FeatureStoreScanFastPath, OrWithNonKeyLeafIsNotFastPath) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::OR,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    cmp("data", NdbScanFilter::COND_GE, 18));
  std::vector<PkBranch> branches;
  EXPECT_FALSE(TryRewriteToPkBranches(root, md, branches));
  EXPECT_TRUE(branches.empty());
}

TEST(FeatureStoreScanFastPath, NandIsNotFastPath) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::NAND,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    cmp("data", NdbScanFilter::COND_GE, 18));
  std::vector<PkBranch> branches;
  EXPECT_FALSE(TryRewriteToPkBranches(root, md, branches));
}

TEST(FeatureStoreScanFastPath, ValueToJson) {
  Node::ParsedValue v;
  v.kind = Node::ParsedValue::Kind::STRING;
  v.s = "a\"b\\c";
  EXPECT_EQ(str(FilterValueToJson(v)), "\"a\\\"b\\\\c\"");
  v.kind = Node::ParsedValue::Kind::UINT64;
  v.u64 = 18446744073709551615ULL;
  EXPECT_EQ(str(FilterValueToJson(v)), "18446744073709551615");
  v.kind = Node::ParsedValue::Kind::DOUBLE;
  v.d = 1.5;
  EXPECT_EQ(str(FilterValueToJson(v)), "1.5");
}

TEST(FeatureStoreScanDedup, ExactDuplicatesCollapseKeepingOrder) {
  auto md = makeMetadata({"id"});
  auto root = logic(NdbScanFilter::OR,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    logic(NdbScanFilter::OR,
                          cmp("id", NdbScanFilter::COND_EQ, 2),
                          cmp("id", NdbScanFilter::COND_EQ, 1)));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 3u);
  ASSERT_TRUE(DedupPkBranches(branches));
  ASSERT_EQ(branches.size(), 2u);
  EXPECT_EQ(str(branches[0].entries["id"]), "1");
  EXPECT_EQ(str(branches[1].entries["id"]), "2");
}

TEST(FeatureStoreScanDedup, SpellingsOfOneKeyCollapse) {
  auto md = makeMetadata({"id"});
  /* 1, "1" and 1.0 select the same row: one branch, first spelling kept */
  auto root = logic(NdbScanFilter::OR,
                    cmp("id", NdbScanFilter::COND_EQ, 1),
                    logic(NdbScanFilter::OR,
                          cmpStr("id", "1"),
                          logic(NdbScanFilter::OR,
                                cmpDbl("id", 1.0),
                                cmpDbl("id", 1.5))));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  ASSERT_EQ(branches.size(), 4u);
  ASSERT_TRUE(DedupPkBranches(branches));
  ASSERT_EQ(branches.size(), 2u);
  EXPECT_EQ(str(branches[0].entries["id"]), "1");
  EXPECT_EQ(str(branches[1].entries["id"]), "1.5");
}

TEST(FeatureStoreScanDedup, SameKeyWithResidualIsNotFastPath) {
  auto md = makeMetadata({"id"});
  /* (id = 1 AND data > 5) OR (id = 1 AND data < 3): the row qualifies if
   * either residual holds, which a single pk read cannot express */
  auto root = logic(NdbScanFilter::OR,
                    logic(NdbScanFilter::AND,
                          cmp("id", NdbScanFilter::COND_EQ, 1),
                          cmp("data", NdbScanFilter::COND_GT, 5)),
                    logic(NdbScanFilter::AND,
                          cmp("id", NdbScanFilter::COND_EQ, 1),
                          cmp("data", NdbScanFilter::COND_LT, 3)));
  std::vector<PkBranch> branches;
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  EXPECT_FALSE(DedupPkBranches(branches));

  /* bare key OR (same key AND residual): the residual would be dropped */
  root = logic(NdbScanFilter::OR,
               cmp("id", NdbScanFilter::COND_EQ, 1),
               logic(NdbScanFilter::AND,
                     cmp("id", NdbScanFilter::COND_EQ, 1),
                     cmp("data", NdbScanFilter::COND_LT, 3)));
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  EXPECT_FALSE(DedupPkBranches(branches));

  /* different keys with residuals are fine */
  root = logic(NdbScanFilter::OR,
               logic(NdbScanFilter::AND,
                     cmp("id", NdbScanFilter::COND_EQ, 1),
                     cmp("data", NdbScanFilter::COND_GT, 5)),
               logic(NdbScanFilter::AND,
                     cmp("id", NdbScanFilter::COND_EQ, 2),
                     cmp("data", NdbScanFilter::COND_LT, 3)));
  ASSERT_TRUE(TryRewriteToPkBranches(root, md, branches));
  EXPECT_TRUE(DedupPkBranches(branches));
  EXPECT_EQ(branches.size(), 2u);
}

TEST(FeatureStoreScanResolve, PrefixedAndBareNamesResolveToPhysicalColumn) {
  auto md = makeMetadata({"id"});
  ScanReadParams scan("", "");
  scan.filterRoot = logic(NdbScanFilter::AND,
                          cmp("root_data", NdbScanFilter::COND_GE, 1),
                          cmp("id", NdbScanFilter::COND_EQ, 1));
  IndexScanParams index;
  index.name = "idx";
  index.columns = {"root_data"};
  scan.index = std::move(index);
  EXPECT_EQ(ResolveScanColumns(scan, md), nullptr);
  EXPECT_EQ(scan.filterRoot->children[0]->column, "data");
  EXPECT_EQ(scan.filterRoot->children[1]->column, "id");
  EXPECT_EQ(scan.index->columns[0], "data");
}

TEST(FeatureStoreScanResolve, JoinedAndUnknownColumnsAreRejected) {
  auto md = makeMetadata({"id"});
  ScanReadParams scan("", "");
  scan.filterRoot = cmp("fg2_data", NdbScanFilter::COND_EQ, 1);
  auto err = ResolveScanColumns(scan, md);
  ASSERT_NE(err, nullptr);
  EXPECT_EQ(err->GetCode(), FEATURE_NOT_IN_ROOT_FG->GetCode());
  EXPECT_NE(err->GetMessage().find("joined feature group"), std::string::npos);

  scan.filterRoot = cmp("nope", NdbScanFilter::COND_EQ, 1);
  err = ResolveScanColumns(scan, md);
  ASSERT_NE(err, nullptr);
  EXPECT_EQ(err->GetCode(), FEATURE_NOT_IN_ROOT_FG->GetCode());
}

TEST(FeatureStoreScanResolve, ComplexColumnsAreRejected) {
  auto md = makeMetadata({"id"});
  ScanReadParams scan("", "");
  scan.filterRoot = isNotNull("tags");
  auto err = ResolveScanColumns(scan, md);
  ASSERT_NE(err, nullptr);
  EXPECT_EQ(err->GetCode(), FILTER_ON_COMPLEX_FEATURE->GetCode());
}
