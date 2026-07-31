/*
 * Copyright (C) 2023 Hopsworks AB
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301,
 * USA.
 */
package testclient

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/testutils"
)

func SendHttpRequest(
	t testing.TB,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, []byte) {
	client := testutils.SetupHttpClient(t)
	return SendHttpRequestWithClient(t, client, httpVerb, url, body, expectedErrMsg, expectedStatus...)
}

// SendHttpRequestWithAPIKey is SendHttpRequest with a caller-chosen API key
// instead of the default testutils.HOPSWORKS_TEST_API_KEY. Used by the
// sharing tests, where authorization differs per user key.
func SendHttpRequestWithAPIKey(
	t testing.TB,
	apiKey string,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, []byte) {
	client := testutils.SetupHttpClient(t)
	return sendHttpRequestWithClientAndAPIKey(t, client, apiKey, httpVerb, url, body,
		expectedErrMsg, expectedStatus...)
}

func SendHttpRequestWithClient(
	t testing.TB,
	client *http.Client,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, []byte) {
	return sendHttpRequestWithClientAndAPIKey(t, client, testutils.HOPSWORKS_TEST_API_KEY,
		httpVerb, url, body, expectedErrMsg, expectedStatus...)
}

func sendHttpRequestWithClientAndAPIKey(
	t testing.TB,
	client *http.Client,
	apiKey string,
	httpVerb string,
	url string,
	body string,
	expectedErrMsg string,
	expectedStatus ...int,
) (int, []byte) {
	t.Helper()

	var req *http.Request
	var resp *http.Response
	var err error
	switch httpVerb {
	case http.MethodPost:
		req, err = http.NewRequest(http.MethodPost, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

	case http.MethodGet:
		req, err = http.NewRequest(http.MethodGet, url, nil)

	case http.MethodDelete:
		req, err = http.NewRequest(http.MethodDelete, url, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

	default:
		t.Fatalf("HTTP verb '%s' is not implemented", httpVerb)
	}

	if err != nil {
		t.Fatalf("failed to create request; error: %v", err)
	}

	conf := config.GetAll()
	if conf.Security.APIKey.UseHopsworksAPIKeys {
		req.Header.Set(config.API_KEY_NAME, apiKey)
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
