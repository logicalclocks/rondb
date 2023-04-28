/*
* This file is part of the RonDB REST API Server
* Copyright (c) 2023 Hopsworks AB
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
package testclient

import (
	"io"
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
) (int, []byte) {
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
	if conf.Security.APIKey.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, testutils.HOPSWORKS_TEST_API_KEY)
	}

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("failed to perform HTTP request towards url: '%s'\nrequest body: '%s'\nerror: %v", url, body, err)
	}
	defer resp.Body.Close()

	respCode := resp.StatusCode
	respBodyBtyes, err := io.ReadAll(resp.Body)
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

	return respCode, respBodyBtyes
}
