/*
 * Copyright (c) 2023, 2024, Hopsworks and/or its affiliates.
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

#ifndef STORAGE_NDB_REST_SERVER2_SERVER_SRC_JSON_PARSER_HPP_
#define STORAGE_NDB_REST_SERVER2_SERVER_SRC_JSON_PARSER_HPP_

#include "config_structs.hpp"
#include "constants.hpp"
#include "pk_data_structs.hpp"
#include "ronsql_data_structs.hpp"
#include "feature_store_data_structs.hpp"

#define SIMDJSON_VERBOSE_LOGGING 0
#include <simdjson.h>

// One JSONParser will be allocated per thread.
class JSONParser {
 private:
  /*
    A parser may have at most one document open at a time
    By design, you should only have one document instance per JSON document.
    For best performance, a parser instance should be reused over several files:
    otherwise you will needlessly reallocate memory, an expensive process.
    If you need to have several documents active at once,
    you should have several parser instances.
  */
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  /*
    We initialize and pre-allocate Internal.batchMaxSize number of char* string buffers
    for each thread, which we could reuse when passing to parser.iterate().
  */
  std::unique_ptr<char[]> buffer;

 public:
  JSONParser();
  std::unique_ptr<char[]> &get_buffer();
  RS_Status pk_parse(simdjson::padded_string_view, PKReadParams &);
  RS_Status batch_parse(simdjson::padded_string_view, std::vector<PKReadParams> &);
  // The config file can specify the number of threads, which is necessary to
  // initialize global jsonParsers variable. Therefore, we'd rather use a static
  // function for parsing the config itself.
  static RS_Status config_parse(const std::string &, AllConfigs &) noexcept;
  RS_Status ronsql_parse(simdjson::padded_string_view, RonSQLParams &);
  RS_Status feature_store_parse(simdjson::padded_string_view,
                                feature_store_data_structs::FeatureStoreRequest &);
  RS_Status batch_feature_store_parse(
    simdjson::padded_string_view,
    feature_store_data_structs::BatchFeatureStoreRequest &);
};

extern JSONParser* jsonParsers;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_JSON_PARSER_HPP_
