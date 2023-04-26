package testclient

import (
	"strconv"
	"testing"

	"google.golang.org/grpc/status"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/pkg/api"
)

func GetColumnDataFromGRPC(t testing.TB, colName string, pkResponse *api.PKReadResponseGRPC) (*string, bool) {
	t.Helper()
	val, ok := (*pkResponse.Data)[colName]
	if !ok {
		return nil, ok
	}
	return val, ok
}

func GetColumnDataFromJson(t testing.TB, colName string, pkResponse *api.PKReadResponseJSON) (*string, bool) {
	t.Helper()

	kvMap := make(map[string]*string)
	for colName, colValue := range *pkResponse.Data {
		if colValue == nil {
			kvMap[colName] = nil
			continue
		}
		value := string([]byte(*colValue))
		if value[0] == '"' {
			var err error
			value, err = strconv.Unquote(value)
			if err != nil {
				t.Fatal(err)
			}
		}
		kvMap[colName] = &value
	}

	val, ok := kvMap[colName]
	if !ok {
		return nil, ok
	}
	return val, ok
}

func GetStatusCodeFromError(t testing.TB, err error) int {
	status, ok := status.FromError(err)
	if !ok {
		t.Fatalf("could not find gRPC status in error: %v", err)
	}

	return common.GrpcCodeToHttpStatus(status.Code())
}
