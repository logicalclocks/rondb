package testclient

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/testutils"
)

func SendHttpRequest(
	t testing.TB,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, string) {
	t.Helper()

	client := testutils.SetupHttpClient(t)
	var req *http.Request
	var resp *http.Response
	var err error
	switch httpVerb {
	case http.MethodPost:
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

	case http.MethodGet:
		req, err = http.NewRequest(http.MethodGet, url, nil)

	default:
		t.Fatalf("HTTP verb '%s' is not implemented", httpVerb)
	}

	if err != nil {
		t.Fatalf("failed to create request; error: %v", err)
	}

	conf := config.GetAll()
	if conf.Security.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("failed to perform HTTP request towards url: '%s'\nrequest body: '%s'\nerror: %v", url, body, err)
	}
	defer resp.Body.Close()

	respCode := resp.StatusCode
	respBodyBtyes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read HTTP response body for url: '%s'\nrequest body: '%s'\nresponse code: %d\nerror: %v", url, body, respCode, err)
	}
	respBody := string(respBodyBtyes)

	idx := -1
	for i, c := range expectedStatus {
		if c == respCode {
			idx = i
		}
	}
	if idx == -1 {
		t.Fatalf("received unexpected status '%d'\nexpected status: '%v'\nurl: '%s'\nbody: '%s'\nresponse body: %v ", respCode, expectedStatus, url, body, respBody)
	}

	if respCode != http.StatusOK && !strings.Contains(respBody, expectedErrMsg) {
		t.Fatalf("response error body does not contain '%s'; received response body: '%s'", expectedErrMsg, respBody)
	}

	return respCode, respBody
}
