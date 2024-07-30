/*
 * Copyright (C) 2023 Hopsworks AB
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
#include "feature_store_data_structs.hpp"

#define SIMDJSON_VERBOSE_LOGGING 0
#include <simdjson.h>

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
  simdjson::ondemand::parser parser[DEFAULT_NUM_THREADS];
  simdjson::ondemand::document doc[DEFAULT_NUM_THREADS];
  /*
    We initialize and pre-allocate Internal.batchMaxSize number of char* string buffers
    for each thread, which we could reuse when passing to parser.iterate().
  */
  std::unique_ptr<char[]> buffers[DEFAULT_NUM_THREADS];

 public:
  JSONParser();
  std::unique_ptr<char[]> &get_buffer(size_t);
  RS_Status pk_parse(size_t, simdjson::padded_string_view, PKReadParams &);
  RS_Status batch_parse(size_t, simdjson::padded_string_view, std::vector<PKReadParams> &);
  RS_Status config_parse(const std::string &, AllConfigs &);
  RS_Status feature_store_parse(size_t, simdjson::padded_string_view,
                                feature_store_data_structs::FeatureStoreRequest &);
};

extern JSONParser jsonParser;

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_JSON_PARSER_HPP_
