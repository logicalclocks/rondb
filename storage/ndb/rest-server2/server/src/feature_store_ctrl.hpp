/*
 * Copyright (C) 2024 Hopsworks AB
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_

#include "metadata.hpp"
#include "rdrs_dal.h"
#include "constants.hpp"
#include "feature_store_data_structs.hpp"
#include "operations_feature_store.hpp"
#include "feature_store_error_code.hpp"
#include "base_ctrl.hpp"

#include <drogon/drogon.h>
#include <drogon/HttpSimpleController.h>

const std::string JSON_NUMBER  = "NUMBER";
const std::string JSON_STRING  = "STRING";
const std::string JSON_BOOLEAN = "BOOLEAN";
const std::string JSON_NIL     = "NIL";
const std::string JSON_OTHER   = "OTHER";

class FeatureStoreCtrl : public drogon::HttpController<FeatureStoreCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(FeatureStoreCtrl::featureStore, FEATURE_STORE_PATH, drogon::Post);
  METHOD_LIST_END

  static void featureStore(const drogon::HttpRequestPtr &req,
                           std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

extern metadata::FeatureViewMetaDataCache fvMetaCache;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_FEATURE_STORE_CTRL_HPP_
