package feature_store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/hamba/avro/v2"
	"hopsworks.ai/rdrs/internal/feature_store"
	"hopsworks.ai/rdrs/internal/log"
)

func DeserialiseComplexFeature(value *json.RawMessage, complexFeature *feature_store.ComplexFeature) (*interface{}, error) {
	valueString, err := decodeJSONString(value)
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
	// var avroDeserialized interface{}
	avroDeserialized := reflect.New(*complexFeature.Struct).Interface()
	err = avro.Unmarshal(*complexFeature.Schema, jsonDecode, &avroDeserialized)
	if err != nil {
		if log.IsDebug() {
			log.Debugf("Failed to deserialize avro")
		}
		return nil, err
	}

	// disard the top most wapper
	nativeJson := reflect.ValueOf(avroDeserialized).Elem().Field(0).Interface()
	// nativeJson := ConvertAvroToJson(avroDeserialized)
	return &nativeJson, err
}

func decodeJSONString(raw *json.RawMessage) (string, error) {
	// Convert the raw message to a string
	rawStr := string(*raw)
	// Check that the first and last characters are quotes
	if len(rawStr) < 2 || rawStr[0] != '"' || rawStr[len(rawStr)-1] != '"' {
		return "", fmt.Errorf("invalid JSON string format")
	}
	// Remove the surrounding quotes
	unquotedStr := rawStr[1 : len(rawStr)-1]
	// Replace escape sequences with their actual characters
	decodedStr := strings.ReplaceAll(unquotedStr, `\"`, `"`)
	return decodedStr, nil
}

func ConvertAvroToJson(o interface{}) interface{} {
	var out interface{}
	switch o.(type) {
	case map[string]interface{}: // union or map
		{
			m := o.(map[string]interface{})
			for key := range m {
				switch m[key].(type) {
				case map[string]interface{}:
					{
						result := make(map[string]interface{})
						structValue := m[key].(map[string]interface{})
						for structKey := range structValue {
							result[structKey] = ConvertAvroToJson(structValue[structKey])
						}
						out = result
					}
				case []interface{}:
					{
						result := make([]interface{}, 0)
						for _, item := range m[key].([]interface{}) {
							itemJson := ConvertAvroToJson(item)
							result = append(result, itemJson)
						}
						out = result
					}
				default:
					{
						out = ConvertAvroToJson(m[key])
					}
				}
			}
		}
	case []interface{}:
		{
			result := make([]interface{}, 0)
			for _, item := range o.([]interface{}) {
				itemJson := ConvertAvroToJson(item)
				result = append(result, itemJson)
			}
			out = result
		}
	default:
		{
			out = o
		}
	}
	return out
}
