package testclient

import (
	"strconv"
	"testing"

	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/pkg/api"
)

func ParseColumnDataFromGRPC(t testing.TB, pkResponse *api.PKReadResponseGRPC, isBinaryData bool) map[string]*string {
	t.Helper()

	kvMap := make(map[string]*string)
	if pkResponse == nil || pkResponse.Data == nil {
		return kvMap
	}

	for colName, colValue := range pkResponse.Data {
		if colValue == nil {
			kvMap[colName] = nil
			continue
		}

		value, err := unquoteIfQuoted(*colValue)
		if err != nil {
			t.Fatal(err)
		}
		kvMap[colName] = &value
	}

	return kvMap
}

func ParseColumnDataFromJson(t testing.TB, pkResponse api.PKReadResponseJSON, isBinaryData bool) map[string]*string {
	t.Helper()

	kvMap := make(map[string]*string)
	if len(pkResponse.Data) < 1 {
		return kvMap
	}

	for colName, colValue := range pkResponse.Data {
		if colValue == nil {
			kvMap[colName] = nil
			continue
		}

		value := string([]byte(*colValue))
		value, err := unquoteIfQuoted(value)
		if err != nil {
			t.Fatal(err)
		}
		kvMap[colName] = &value
	}

	return kvMap
}

func unquoteIfQuoted(value string) (string, error) {
	if value[0] != '"' {
		return value, nil
	}
	return strconv.Unquote(value)
}

func GetStatusCodeFromError(t testing.TB, err error) int {
	status, ok := status.FromError(err)
	if !ok {
		t.Fatalf("could not find gRPC status in error: %v", err)
	}

	return common.GrpcCodeToHttpStatus(status.Code())
}
