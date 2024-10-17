/*
 * Copyright (c) 2024, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_

#include <drogon/drogon.h>
#include "constants.hpp"
#include "status.hpp"
#include "ronsql_data_structs.hpp"
#include "storage/ndb/src/ronsql/RonSQLCommon.hpp"

class RonSQLCtrl : public drogon::HttpController<RonSQLCtrl> {
 public:
  METHOD_LIST_BEGIN
  ADD_METHOD_TO(RonSQLCtrl::ronsql, RONSQL_PATH, drogon::Post);
  METHOD_LIST_END

  static void ronsql(const drogon::HttpRequestPtr &req,
                     std::function<void(const drogon::HttpResponsePtr &)> &&callback);
};

RS_Status ronsql_validate_database_name(std::string& database);

RS_Status ronsql_validate_and_init_params(RonSQLParams& input,
                                          RonSQLExecParams& ep,
                                          std::ostringstream* out_stream,
                                          std::ostringstream* err_stream,
                                          ArenaMalloc* amalloc,
                                          bool* do_explain);

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_RONSQL_CTRL_HPP_
