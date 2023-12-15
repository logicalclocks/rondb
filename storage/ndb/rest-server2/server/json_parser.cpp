#include "json_parser.hpp"
#include "pk_data_structs.hpp"
#include "encoding.hpp"

char buffers[MAX_THREADS][BODY_MAX_SIZE];

int json_parser::parse(std::string_view& reqBody, PKReadParams& reqStruct, int threadId) {
  char* json = buffers[threadId];

  if (reqBody.size() >= (BODY_MAX_SIZE - SIMDJSON_PADDING)) {
    std::cout << "Parsing request failed. Error: " << simdjson::error_code::INSUFFICIENT_PADDING
      << " at " << __LINE__ << std::endl;
    return simdjson::error_code::INSUFFICIENT_PADDING;
  }

  memcpy(json, reqBody.data(), reqBody.size());
  json[reqBody.size()] = 0;
  simdjson::padded_string paddedJson(json, reqBody.size());

  ondemand::parser parser;
  ondemand::document doc;

  auto error = parser.iterate(paddedJson).get(doc);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  ondemand::object reqObject;
  error = doc.get_object().get(reqObject);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  ondemand::array filters;
  error = reqObject["filters"].get_array().get(filters);
  if (error) {
    std::cout << "Parsing request failed. Error: " << error
      << " at " << doc.current_location() << std::endl;
    return error;
  }

  for (auto filter: filters) {
    PKReadFilter pkReadFilter;
    ondemand::object filterObj;
    error = filter.get(filterObj);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }

    std::string_view column;
    error = filterObj["column"].get(column);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
        throw ValidationError("Field validation for 'Column' failed on the 'required' tag");
      return error;
    }
    pkReadFilter.column = column;

    ondemand::value value;
    error = filterObj["value"].get(value);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }

    std::ostringstream oss;
    oss << value;
    std::string valueJson = oss.str();
    std::vector<char> bytes(valueJson.begin(), valueJson.end());

    pkReadFilter.value = bytes;

    reqStruct.filters.emplace_back(pkReadFilter);
  }

  ondemand::array readColumns;
  ondemand::value readColumnsVal = reqObject["readColumns"];
  if (readColumnsVal.is_null()) {
    readColumns = {};
  } else {
    error = readColumnsVal.get(readColumns);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
    for (auto readColumn: readColumns) {
      PKReadReadColumn pkReadReadColumn;
      ondemand::object readColumnObj;
      error = readColumn.get(readColumnObj);
      if (error) {
        std::cout << "Parsing request failed. Error: " << error
          << " at " << doc.current_location() << std::endl;
        return error;
      }

      std::string_view column;
      error = readColumnObj["column"].get(column);
      if (error) {
        std::cout << "Parsing request failed. Error: " << error
          << " at " << doc.current_location() << std::endl;
        return error;
      }
      pkReadReadColumn.column = column;

      std::string_view dataReturnType;
      ondemand::value dataReturnTypeVal = readColumnObj["dataReturnType"];
      if (dataReturnTypeVal.is_null()) {
        dataReturnType = "";
      } else {
        error = dataReturnTypeVal.get(dataReturnType);
        if (error) {
          std::cout << "Parsing request failed. Error: " << error
            << " at " << doc.current_location() << std::endl;
          return error;
        }
      }
      pkReadReadColumn.returnType = dataReturnType;

      reqStruct.readColumns.emplace_back(pkReadReadColumn);
    }
  }

  std::string_view operationId;
  ondemand::value operationIdVal = reqObject["operationId"];
  if (operationIdVal.is_null()) {
    operationId = "";
  } else {
    error = operationIdVal.get(operationId);
    if (error) {
      std::cout << "Parsing request failed. Error: " << error
        << " at " << doc.current_location() << std::endl;
      return error;
    }
  }

  reqStruct.operationId = operationId;
  return simdjson::SUCCESS;
}
