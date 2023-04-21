package testclient

import (
	"encoding/base64"
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

func ParseColumnDataFromJson(t testing.TB, pkResponse api.PKReadResponseJSON, isBinaryData bool) map[string]*string {
	t.Helper()

	kvMap := make(map[string]*string)
	for colName, colValue := range *pkResponse.Data {
		if colValue == nil {
			kvMap[colName] = nil
			continue
		}
		var value string
		if isBinaryData {
			value = base64.StdEncoding.EncodeToString(*colValue)
		} else {
			value = string([]byte(*colValue))
			if value[0] == '"' {
				var err error
				value, err = strconv.Unquote(value)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		kvMap[colName] = &value
	}

	return kvMap
}

func GetStatusCodeFromError(t testing.TB, err error) int {
	status, ok := status.FromError(err)
	if !ok {
		t.Fatalf("could not find gRPC status in error: %v", err)
	}

	return common.GrpcCodeToHttpStatus(status.Code())
}
