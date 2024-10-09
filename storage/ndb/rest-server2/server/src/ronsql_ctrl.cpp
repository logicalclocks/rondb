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

#include "ronsql_ctrl.hpp"
#include "error_strings.h"
#include "rdrs_dal.hpp"
#include "json_parser.hpp"
#include <drogon/HttpTypes.h>
#include "storage/ndb/src/ronsql/RonSQLPreparer.hpp"
#include "api_key.hpp"
#include <EventLogger.hpp>

extern EventLogger *g_eventLogger;

#if (defined(VM_TRACE) || defined(ERROR_INSERT))
//#define DEBUG_SQL_CTRL 1
#endif

#ifdef DEBUG_SQL_CTRL
#define DEB_SQL_CTRL(...) do { g_eventLogger->info(__VA_ARGS__); } while (0)
#else
#define DEB_SQL_CTRL(...) do { } while (0)
#endif

using std::endl;

void RonSQLCtrl::ronsql(const drogon::HttpRequestPtr &req,
                        std::function<void(const drogon::HttpResponsePtr &)> &&callback) {
  size_t currentThreadIndex = drogon::app().getCurrentThreadIndex();
  auto resp = drogon::HttpResponse::newHttpResponse();
  if (currentThreadIndex >= globalConfigs.rest.numThreads) {
    resp->setBody("Too many threads");
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    callback(resp);
    return;
  }
  JSONParser& jsonParser = jsonParsers[currentThreadIndex];

  // Store it to the first string buffer
  const char *json_str = req->getBody().data();
  DEB_SQL_CTRL("\n\n JSON REQUEST: \n %s \n", json_str);

  size_t length        = req->getBody().length();
  if (length > globalConfigs.internal.reqBufferSize) {
    resp->setBody("Request too large");
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }
  memcpy(jsonParser.get_buffer().get(), json_str, length);

  RonSQLParams reqStruct;

  RS_Status status = jsonParser.ronsql_parse(
      simdjson::padded_string_view(
        jsonParser.get_buffer().get(),
        length,
        globalConfigs.internal.reqBufferSize + simdjson::SIMDJSON_PADDING),
      reqStruct);

  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  ArenaAllocator aalloc;
  RonSQLExecParams params;

  std::string_view& database = reqStruct.database;
  status = ronsql_validate_database_name(database);
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  if (globalConfigs.security.apiKey.useHopsworksAPIKeys) {
    auto api_key = req->getHeader(API_KEY_NAME_LOWER_CASE);
    status = authenticate(api_key, database);
    if (static_cast<drogon::HttpStatusCode>(status.http_code) !=
          drogon::HttpStatusCode::k200OK) {
      resp->setBody(std::string(status.message));
      resp->setStatusCode((drogon::HttpStatusCode)status.http_code);
      callback(resp);
      return;
    }
  }

  std::ostringstream out_stream;
  std::ostringstream err_stream;

  bool do_explain = false;
  status = ronsql_validate_and_init_params(reqStruct,
                                           params,
                                           &out_stream,
                                           &err_stream,
                                           &aalloc,
                                           &do_explain);
  if (static_cast<drogon::HttpStatusCode>(status.http_code) != drogon::HttpStatusCode::k200OK) {
    resp->setBody(std::string(status.message));
    resp->setStatusCode(drogon::HttpStatusCode::k400BadRequest);
    callback(resp);
    return;
  }

  bool json_output = params.output_format == RonSQLExecParams::OutputFormat::JSON ||
                     params.output_format == RonSQLExecParams::OutputFormat::JSON_ASCII;
  if (json_output) {
    if (reqStruct.operationId.empty()) {
      out_stream << "{\"data\":\n";
    }
    else {
      out_stream << "{\"operationId\": \"" << reqStruct.operationId << "\",\n\"data\":\n";
    }
  }

  status = ronsql_dal(database.data(), &params);

  if (json_output) {
    out_stream << "}\n";
  }

  std::string out_str = out_stream.str();
  std::string err_str = err_stream.str();
#ifdef VM_TRACE
  bool hasOut = !out_str.empty();
  bool hasErr = !err_str.empty();
#endif
  if (static_cast<drogon::HttpStatusCode>(status.http_code) == drogon::HttpStatusCode::k200OK) {
    assert(!hasErr);
    assert(hasOut);
    switch (params.output_format) {
    case RonSQLExecParams::OutputFormat::JSON:
      resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
      break;
    case RonSQLExecParams::OutputFormat::JSON_ASCII:
      /*
       * Although JSON is described as a text-based format, it requires the
       * UTF-8 encoding [1]. Since one cannot choose a text encoding, it may
       * be considered a binary format. Indeed, the application/json media
       * type is registerd as a binary format [2], and therefore has no
       * charset parameter. Despite this, many servers supply a
       * `charset=utf-8` parameter, including drogon [3]. The intention behind
       * RonSQLExecParams::OutputFormat::JSON_ASCII is to provide a format that
       * only uses ASCII characters, so we communicate this using a
       * `charset=US-ASCII` parameter. This may help a RonSQL aware client to
       * confirm ASCII-only content. It is not expected to cause any trouble for
       * unaware clients, as a compliant client should ignore the charset
       * parameter altogether and use utf-8 to decode the JSON object, which
       * will work since UTF-8 is backwards compatible with ASCII.
       *
       * [1] https://www.rfc-editor.org/rfc/rfc8259#section-8.1
       * [2] https://www.iana.org/assignments/media-types/application/json
       * [3] ../../extra/drogon/drogon-1.8.7/lib/src/HttpUtils.cc:565
       */
      resp->setContentTypeCodeAndCustomString(
        drogon::CT_APPLICATION_JSON,
        "content-type: application/json; charset=US-ASCII\r\n");
      break;
    case RonSQLExecParams::OutputFormat::TEXT:
      [[fallthrough]];
    case RonSQLExecParams::OutputFormat::TEXT_NOHEADER:
      if (do_explain) {
        /*
         * The text/plain media type technically requires CRLF line endings.
         * This output instead uses Unix line endings (LF). There seems to be
         * no way to specify this in the content type.
         */
        resp->setContentTypeCodeAndCustomString
          (drogon::CT_TEXT_PLAIN,
           "content-type: text/plain; charset=utf-8\r\n");
      }
      else {
        /*
         * The text/tab-separated-values media type [1] is unfortunately a
         * little lack-luster. It has no mechanism to specify whether a header
         * row is present, and also requires an "Encoding type" parameters
         * without describing it. Here we do the best we can by using the
         * parameter definitions from the specification for the text/csv media
         * type [2] instead.
         *
         * [1] https://www.iana.org/assignments/media-types/text/tab-separated-values
         * [2] https://www.iana.org/assignments/media-types/text/csv
         */
        resp->setContentTypeCodeAndCustomString
          (drogon::CT_CUSTOM,
           params.output_format == RonSQLExecParams::OutputFormat::TEXT
           ? "content-type: text/tab-separated-values; charset=utf-8; header=present\r\n"
           : "content-type: text/tab-separated-values; charset=utf-8; header=absent\r\n"
           );
      }
      break;
    default:
      // Should be unreachable
      abort();
    }
    resp->setBody(out_str);
    resp->setStatusCode(drogon::HttpStatusCode::k200OK);
  }
  else {
    resp->setStatusCode(drogon::HttpStatusCode::k500InternalServerError);
    resp->setContentTypeCodeAndCustomString(
      drogon::CT_TEXT_PLAIN, "content-type: text/plain; charset=utf-8; \r\n");
    resp->setBody(err_str);
  }
  callback(resp);
}

RS_Status ronsql_validate_database_name(std::string_view& database) {
  RS_Status status = validate_db_identifier(database);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK)) {
    if (status.code == ERROR_CODE_EMPTY_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_EMPTY_IDENTIFIER,
                        ERROR_049).status;
    }
    if (status.code == ERROR_CODE_IDENTIFIER_TOO_LONG) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_MAX_DB, ERROR_050).status;
    }
    if (status.code == ERROR_CODE_INVALID_IDENTIFIER) {
      return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                        ERROR_CODE_INVALID_IDENTIFIER, ERROR_051).status;
    }
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_DB_NAME,
                      (std::string(ERROR_051) + "; error: " + status.message).c_str()).status;
  }
  return RS_OK;
}

RS_Status ronsql_validate_and_init_params(RonSQLParams& req,
                                          RonSQLExecParams& ep,
                                          std::ostringstream* out_stream,
                                          std::ostringstream* err_stream,
                                          ArenaAllocator* aalloc,
                                          bool* do_explain) {
  // req.query -> ep.sql_buffer and ep.sql_len
  assert(aalloc != NULL);
  ep.sql_len = req.query.length(); // todo-ronsql what if length is not in bytes?
  assert(ep.sql_buffer == NULL);
  ep.sql_buffer = aalloc->alloc<char>(ep.sql_len + 2); // todo-ronsql catch OOM exception
  memcpy(ep.sql_buffer, req.query.c_str(), req.query.length());
  ep.sql_buffer[ep.sql_len++] = '\0';
  ep.sql_buffer[ep.sql_len++] = '\0';
  // aalloc -> ep.aalloc
  assert(ep.aalloc == NULL);
  ep.aalloc = aalloc;
  // req.explainMode -> ep.explain_mode
  if (req.explainMode == "ALLOW") {
    ep.explain_mode = RonSQLExecParams::ExplainMode::ALLOW;
  }
  else if (req.explainMode == "FORBID") {
    ep.explain_mode = RonSQLExecParams::ExplainMode::FORBID;
  }
  else if (req.explainMode == "REQUIRE") {
    ep.explain_mode = RonSQLExecParams::ExplainMode::REQUIRE;
  }
  else if (req.explainMode == "REMOVE") {
    ep.explain_mode = RonSQLExecParams::ExplainMode::REMOVE;
  }
  else if (req.explainMode == "FORCE") {
    ep.explain_mode = RonSQLExecParams::ExplainMode::FORCE;
  }
  else {
    return RS_CLIENT_ERROR("Invalid explainMode");
  }
  // out_stream -> ep.out_stream
  assert(ep.out_stream == NULL);
  assert(out_stream != NULL);
  ep.out_stream = out_stream;
  // req.outputFormat -> ep.output_format
  if (req.outputFormat == "JSON") {
    ep.output_format = RonSQLExecParams::OutputFormat::JSON;
  }
  else if (req.outputFormat == "JSON_ASCII") {
    ep.output_format = RonSQLExecParams::OutputFormat::JSON_ASCII;
  }
  else if (req.outputFormat == "TEXT") {
    ep.output_format = RonSQLExecParams::OutputFormat::TEXT;
  }
  else if (req.outputFormat == "TEXT_NOHEADER") {
    ep.output_format = RonSQLExecParams::OutputFormat::TEXT_NOHEADER;
  }
  else {
    return RS_CLIENT_ERROR("Invalid outputFormat");
  }
  // err_stream -> ep.err_stream
  assert(ep.err_stream == NULL);
  assert(err_stream != NULL);
  ep.err_stream = err_stream;
  // req.operationId -> ep.operation_id
  RS_Status status = validate_operation_id(req.operationId);
  if (status.http_code != static_cast<HTTP_CODE>(drogon::HttpStatusCode::k200OK))
    return CRS_Status(static_cast<HTTP_CODE>(drogon::HttpStatusCode::k400BadRequest),
                      ERROR_CODE_INVALID_OPERATION_ID,
                      (std::string(ERROR_055) + "; error: " + status.message).c_str()).status;
  if (!req.operationId.empty()) {
    ep.operation_id = req.operationId.c_str();
    if (ep.output_format == RonSQLExecParams::OutputFormat::TEXT)
      return RS_CLIENT_ERROR("operationId not supported with output format TEXT");
    if (ep.output_format == RonSQLExecParams::OutputFormat::TEXT_NOHEADER)
      return RS_CLIENT_ERROR("operationId not supported with output format TEXT_NOHEADER");
  }
  // do_explain -> ep.do_explain
  assert(do_explain != NULL);
  ep.do_explain = do_explain;
  // Everything ok
  return RS_OK;
}
