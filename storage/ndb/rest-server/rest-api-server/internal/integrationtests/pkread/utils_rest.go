package pkread

import (
	"encoding/json"
	"net/http"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func pkRESTTest(t testing.TB, testInfo api.PKTestInfo, isBinaryData bool, validate bool) {
	url := testutils.NewPKReadURL(testInfo.Db, testInfo.Table)
	body, err := json.MarshalIndent(testInfo.PkReq, "", "\t")
	if err != nil {
		t.Fatalf("Failed to marshall test request %v", err)
	}

	httpCode, res := testclient.SendHttpRequest(
		t,
		config.PK_HTTP_VERB,
		url,
		string(body),
		testInfo.ErrMsgContains,
		testInfo.HttpCode,
	)
	if httpCode == http.StatusOK && validate {
		validateResHttp(t, testInfo, res, isBinaryData)
	}
}

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
