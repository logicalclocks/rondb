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

	// dicsard the top most wapper
	nativeJson := reflect.ValueOf(avroDeserialized).Elem().Field(0).Interface()
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
