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

#include "config_structs.hpp"
#include "json_parser.hpp"
#include "constants.hpp"
#include "mysql_com.h"
#include "rdrs_dal.hpp"

#include <cstdlib>
#include <drogon/HttpAppFramework.h>
#include <drogon/HttpTypes.h>
#include <mutex>
#include <simdjson.h>
#include <sstream>
#include <string_view>
#include <utility>
#include <fstream>

AllConfigs globalConfigs;
std::mutex globalConfigsMutex;

bool isUnitTest() {
  const char *env_var = std::getenv("RUNNING_UNIT_TESTS");
  return (env_var != nullptr && std::string(env_var) == "1");
}

/*
 * Default constructors with default values
 */

#define REMOVE_FIRST_COMMA(IGNORE, ...) __VA_ARGS__
#define CLASS(NAME, ...) NAME::NAME() : REMOVE_FIRST_COMMA(__VA_ARGS__) {}
#define CM(DATATYPE, VARIABLENAME, JSONKEYNAME, INITEXPR) ,VARIABLENAME(INITEXPR)
#define PROBLEM(CONDITION, MESSAGE)
#define CLASSDEFS(...)
#define VECTOR(DATATYPE)

#include "config_structs_def.hpp"

#undef CLASS
#undef CM
#undef PROBLEM
#undef CLASSDEFS
#undef VECTOR

/*
 * Validation functions
 */

class ConfigValidationError : public std::runtime_error {
public:
  ConfigValidationError(std::string message) : std::runtime_error("ConfigValidationError"), m_error_message(message) {}
  std::string m_error_message;
};

#define DEFINE_VALIDATOR(NAME, ...) void validate([[maybe_unused]] NAME value) __VA_ARGS__
#define CLASS(NAME, ...) DEFINE_VALIDATOR(NAME, { __VA_ARGS__ })
#define CM(DATATYPE, VARIABLENAME, JSONKEYNAME, INITEXPR) DATATYPE& VARIABLENAME = value.VARIABLENAME; validate(VARIABLENAME);
#define PROBLEM(CONDITION, MESSAGE) if (CONDITION) { throw ConfigValidationError(MESSAGE); }
#define CLASSDEFS(...)
#define VECTOR(DATATYPE) DEFINE_VALIDATOR(std::vector<DATATYPE>, { for (DATATYPE& elem : value) validate(elem); })

DEFINE_VALIDATOR(uint32_t, {})
DEFINE_VALIDATOR(std::string, {})

#include "config_structs_def.hpp"

#undef CLASS
#undef CM
#undef PROBLEM
#undef CLASSDEFS
#undef VECTOR

/*
 * Other methods
 */

std::string RonDB::generate_Mgmd_connect_string() {
  Mgmd mgmd = Mgmds[0];
  return mgmd.IP + ":" + std::to_string(mgmd.port);
}

std::string MySQL::generate_mysqld_connect_string() {
  // user:password@tcp(IP:Port)/
  return user + ":" + password + "@tcp(" + servers[0].IP + ":" +
         std::to_string(servers[0].port) + ")/";
}

AllConfigs AllConfigs::get_all() {
  return globalConfigs;
}

RS_Status AllConfigs::set_all(AllConfigs newConfigs) {
  std::lock_guard<std::mutex> lock(globalConfigsMutex);
  try {
    validate(newConfigs);
  }
  catch (ConfigValidationError &e) {
    return CRS_Status(
               static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
               ("Failed validating config; error: " + e.m_error_message).c_str())
        .status;
  }
  globalConfigs = newConfigs;
  return CRS_Status::SUCCESS.status;
}

RS_Status AllConfigs::set_from_file(const std::string &configFile) {
  AllConfigs newConfigs;
  // Read config file
  std::ifstream file(configFile);
  if (!file) {
    return CRS_Status(
               static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
               ("failed reading config file " + configFile +"; error: " + std::string(strerror(errno))).c_str())
        .status;
  }
  std::string configStr((std::istreambuf_iterator<char>(file)), {});

  file.close();

  // Parse config file.
  RS_Status status = JSONParser::config_parse(configStr, newConfigs);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    return status;
  }

  return set_all(newConfigs);
}

RS_Status AllConfigs::init(std::string configFile) {
  if (configFile.empty()) {
    // Set to defaults
    AllConfigs newConfigs = AllConfigs();
    return set_all(newConfigs);
  }
  return set_from_file(configFile);
}
