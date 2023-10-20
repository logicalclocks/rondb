package feature_store

import (
	"encoding/base64"
	"encoding/json"
	"strings"

	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/log"
)

func DeserialiseComplexFeature(value *json.RawMessage, decoder *feature_store.AvroDecoder) (*interface{}, error) {
	var valueString string
	err := json.Unmarshal(*value, &valueString)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to unmarshal. Value: %s", valueString)
		}
		return nil, err
	}
	jsonDecode, err := base64.StdEncoding.DecodeString(valueString)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to decode base64. Value: %s", valueString)
		}
		return nil, err
	}
	native, err := decoder.Decode(jsonDecode)
	nativeJson := ConvertAvroToJson(native)
	return &nativeJson, err
}

func ConvertAvroToJson(o interface{}) interface{} {
	var out interface{}
	switch o.(type) {
	case map[string]interface{}: // union or map
		m := o.(map[string]interface{})
		for key := range m {
			switch strings.Split(key, ".")[0] {
			case "struct":
				result := make(map[string]interface{})
				structValue := m[key].(map[string]interface{})
				for structKey := range structValue {
					result[structKey] = ConvertAvroToJson(structValue[structKey])
				}
				out = result
			case "array":
				result := make([]interface{}, 0)
				for _, item := range m[key].([]interface{}) {
					itemJson := ConvertAvroToJson(item)
					result = append(result, itemJson)
				}
				out = result
			default:
				out = ConvertAvroToJson(m[key])
			}
		}
	case []interface{}:
		result := make([]interface{}, 0)
		for _, item := range o.([]interface{}) {
			itemJson := ConvertAvroToJson(item)
			result = append(result, itemJson)
		}
		out = result
	default:
		out = o
	}
	return out
}
