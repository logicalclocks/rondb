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

#define SIMDJSON_VERBOSE_LOGGING 0

#include "pk_data_structs.hpp"

#include <simdjson.h>

namespace json_parser {
RS_Status parse(std::string_view &, PKReadParams &);
RS_Status batch_parse(std::string_view &, std::vector<PKReadParams> &);
}  // namespace json_parser

#endif  // STORAGE_NDB_REST_SERVER2_SERVER_SRC_JSON_PARSER_HPP_
