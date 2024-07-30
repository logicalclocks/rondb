#include "feature_util.hpp"
#include <drogon/HttpTypes.h>
#include <vector>

std::string base64_decode(const std::string &encoded_string) {
  const char *src = encoded_string.c_str();
  size_t src_len  = encoded_string.size();
  std::vector<char> decoded_data(src_len);  // Allocate enough space

  const char *end_ptr = nullptr;
  int flags           = 0;  // No special flags
  int64_t decoded_len = base64_decode(src, src_len, decoded_data.data(), &end_ptr, flags);

  if (decoded_len < 0) {
    throw std::runtime_error("Failed to decode base64 string.");
  }

  return std::string(decoded_data.begin(), decoded_data.begin() + decoded_len);
}

std::tuple<std::vector<char>, std::shared_ptr<RestErrorCode>>
DeserialiseComplexFeature(const std::vector<char> &value, const metadata::AvroDecoder &decoder) {
  std::string valueString(value.begin(), value.end());
  simdjson::dom::parser parser;
  simdjson::dom::element element;

  auto error = parser.parse(valueString).get(element);
  if (error != 0U) {
    return {{},
            std::make_shared<RestErrorCode>("Failed to unmarshal JSON value.",
                                            static_cast<int>(drogon::k500InternalServerError))};
  }

  valueString = element.get_string().value();
  std::string jsonDecode;
  try {
    jsonDecode = base64_decode(valueString);
  } catch (const std::runtime_error &e) {
    return {{},
            std::make_shared<RestErrorCode>("Failed to decode base64 value.",
                                            static_cast<int>(drogon::k400BadRequest))};
  }

  std::vector<uint8_t> binaryData(jsonDecode.begin(), jsonDecode.end());
  avro::GenericDatum native;
  try {
    native = decoder.decode(binaryData);
  } catch (const std::runtime_error &e) {
    return {{},
            std::make_shared<RestErrorCode>(e.what(), static_cast<int>(drogon::k400BadRequest))};
  }

  std::vector<char> nativeJson = ConvertAvroToJson(native);
  return {nativeJson, nullptr};
}

template <typename T> void AppendToVector(std::vector<char> &vec, const T &value) {
  const char *data = reinterpret_cast<const char *>(&value);
  vec.insert(vec.end(), data, data + sizeof(T));
}

void AppendStringToVector(std::vector<char> &vec, const std::string &str) {
  vec.insert(vec.end(), str.begin(), str.end());
}

void AppendBytesToVector(std::vector<char> &vec, const std::vector<uint8_t> &bytes) {
  vec.insert(vec.end(), bytes.begin(), bytes.end());
}

std::vector<char> ConvertAvroToJson(const avro::GenericDatum &datum) {
  std::vector<char> result;

  switch (datum.type()) {
  case avro::AVRO_RECORD: {
    const auto &record = datum.value<avro::GenericRecord>();
    for (size_t i = 0; i < record.fieldCount(); ++i) {
      const auto &field = record.fieldAt(i);
      auto field_data   = ConvertAvroToJson(field);
      result.insert(result.end(), field_data.begin(), field_data.end());
    }
    break;
  }
  case avro::AVRO_ARRAY: {
    const auto &array = datum.value<avro::GenericArray>();
    for (const auto &item : array.value()) {
      auto item_data = ConvertAvroToJson(item);
      result.insert(result.end(), item_data.begin(), item_data.end());
    }
    break;
  }
  case avro::AVRO_MAP: {
    const auto &map = datum.value<avro::GenericMap>();
    for (const auto &item : map.value()) {
      AppendStringToVector(result, item.first);
      auto item_data = ConvertAvroToJson(item.second);
      result.insert(result.end(), item_data.begin(), item_data.end());
    }
    break;
  }
  case avro::AVRO_STRING: {
    const auto &str = datum.value<std::string>();
    AppendStringToVector(result, str);
    break;
  }
  case avro::AVRO_INT: {
    int32_t value = datum.value<int32_t>();
    AppendToVector(result, value);
    break;
  }
  case avro::AVRO_LONG: {
    int64_t value = datum.value<int64_t>();
    AppendToVector(result, value);
    break;
  }
  case avro::AVRO_FLOAT: {
    float value = datum.value<float>();
    AppendToVector(result, value);
    break;
  }
  case avro::AVRO_DOUBLE: {
    double value = datum.value<double>();
    AppendToVector(result, value);
    break;
  }
  case avro::AVRO_BOOL: {
    bool value = datum.value<bool>();
    AppendToVector(result, value);
    break;
  }
  case avro::AVRO_BYTES: {
    const auto &bytes = datum.value<std::vector<uint8_t>>();
    AppendBytesToVector(result, bytes);
    break;
  }
  case avro::AVRO_NULL:
    // No data to append for null values
    break;
  default:
    throw std::runtime_error("Unsupported Avro type");
  }

  return result;
}