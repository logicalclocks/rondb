package pkread

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/pkg/api"
)

func validateResHttp(t testing.TB, testInfo api.PKTestInfo, resp string, isBinaryData bool) {
	t.Helper()
	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		var pkResponse api.PKReadResponseJSON
		err := json.Unmarshal([]byte(resp), &pkResponse)
		if err != nil {
			t.Fatalf("Failed to unmarshal response object %v", err)
		}

		jsonVal, found := testclient.GetColumnDataFromJson(t, key, &pkResponse)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		integrationtests.CompareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, jsonVal, isBinaryData)
	}
}

func validateResGRPC(
	t testing.TB,
	testInfo api.PKTestInfo,
	resp *api.PKReadResponseGRPC,
	isBinaryData bool,
) {
	t.Helper()
	for i := 0; i < len(testInfo.RespKVs); i++ {
		key := string(testInfo.RespKVs[i].(string))

		val, found := testclient.GetColumnDataFromGRPC(t, key, resp)
		if !found {
			t.Fatalf("Key not found in the response. Key %s", key)
		}

		var err error
		if val != nil {
			quotedVal := fmt.Sprintf("\"%s\"", *val) // you have to surround the string with "s
			*val, err = strconv.Unquote(quotedVal)
			if err != nil {
				t.Fatalf("Unquote failed %v\n", err)
			}
		}

		integrationtests.CompareDataWithDB(t, testInfo.Db, testInfo.Table, testInfo.PkReq.Filters,
			&key, val, isBinaryData)
	}
}
