/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2024,2025 Hopsworks AB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, version 3.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package feature_store

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

type ComplexFeature struct {
	Schema *avro.Schema
	Struct *reflect.Type
}

func DeserialiseComplexFeature(t *testing.T, value *json.RawMessage, complexFeature *ComplexFeature) (*interface{}, error) {
	valueString, err := decodeJSONString(value)
	if err != nil {
		t.Fatalf("Failed to unmarshal. Value: %s", valueString)
		return nil, err
	}

	jsonDecode, err := base64.StdEncoding.DecodeString(valueString)
	if err != nil {
		t.Fatalf("Failed to decode base64. Value: %s", valueString)
		return nil, err
	}
	// var avroDeserialized interface{}
	avroDeserialized := reflect.New(*complexFeature.Struct).Interface()
	err = avro.Unmarshal(*complexFeature.Schema, jsonDecode, &avroDeserialized)
	if err != nil {
		t.Fatalf("Failed to deserialize avro")
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
