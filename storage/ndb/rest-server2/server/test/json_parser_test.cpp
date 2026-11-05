/*
 * Copyright (C) 2026 Hopsworks AB
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
 * RONDB-1152: request bodies that simdjson rejects in parser.iterate()
 * itself, before there is a document. Cluster-free.
 *
 * The parse error handler asked the document for the error location. After a
 * failed iterate() the document is not from this request: on a parser that
 * has not parsed anything yet its iterator is null, and one such request
 * killed the server with SIGSEGV, before authentication. Each REST thread
 * owns one parser, so a fresh parser is what the first request on a thread
 * meets.
 */

#include <gtest/gtest.h>

#include <NdbMutex.h>

#include <functional>
#include <string>
#include <vector>

#include "config_structs.hpp"
#include "json_parser.hpp"

/* Owned by main.cc in the server binary; test binaries provide their own
 * (same pattern as error_handling_test.cpp). */
NdbMutex *globalConfigsMutex = nullptr;

namespace {

/* Bodies that fail in iterate(): simdjson finds an unclosed string while
 * indexing the structure, and an empty body has nothing to index. */
const std::vector<std::string> kIterateErrors = {
  R"({"operations":[{"method":"POST","relative-url":"db)",
  R"({"filters":[{"column":"id","value":"1)",
  R"(")",
  "",
};

using Parse = std::function<RS_Status(JSONParser &,
                                      simdjson::padded_string_view)>;

const std::vector<std::pair<const char *, Parse>> kEntryPoints = {
  {"pk_parse", [](JSONParser &p, simdjson::padded_string_view body) {
     PKReadParams req;
     return p.pk_parse(body, req);
   }},
  {"batch_parse", [](JSONParser &p, simdjson::padded_string_view body) {
     std::vector<PKReadParams> req;
     return p.batch_parse(body, req);
   }},
  {"scan_parse", [](JSONParser &p, simdjson::padded_string_view body) {
     ScanReadParams req("db", "table");
     return p.scan_parse(body, req);
   }},
  {"ronsql_parse", [](JSONParser &p, simdjson::padded_string_view body) {
     RonSQLParams req;
     return p.ronsql_parse(body, req);
   }},
  {"feature_store_parse",
   [](JSONParser &p, simdjson::padded_string_view body) {
     feature_store_data_structs::FeatureStoreRequest req;
     return p.feature_store_parse(body, req);
   }},
  {"batch_feature_store_parse",
   [](JSONParser &p, simdjson::padded_string_view body) {
     feature_store_data_structs::BatchFeatureStoreRequest req;
     return p.batch_feature_store_parse(body, req);
   }},
};

// What the controllers do: copy the body into the thread's buffer and parse
// it there.
RS_Status parse(JSONParser &parser, const Parse &entry,
                const std::string &body) {
  parser.copy_request(body.data(), body.size());
  return entry(parser, simdjson::padded_string_view(
    parser.get_buffer().get(), body.size(),
    globalConfigs.internal.maxHttpReqSize + simdjson::SIMDJSON_PADDING));
}

}  // namespace

TEST(JSONParser, IterateErrorOnFreshParserIsAClientError) {
  for (const auto &[name, entry] : kEntryPoints) {
    for (const std::string &body : kIterateErrors) {
      JSONParser parser;
      RS_Status status = parse(parser, entry, body);
      EXPECT_EQ(status.http_code, CLIENT_ERROR)
          << name << " on '" << body << "': " << status.message;
    }
  }
}
