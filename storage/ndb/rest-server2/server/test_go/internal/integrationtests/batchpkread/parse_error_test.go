/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2026 Hopsworks AB
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
package batchpkread

import (
	"net/http"
	"strings"
	"testing"

	"hopsworks.ai/rdrs2/internal/config"
	"hopsworks.ai/rdrs2/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs2/internal/testutils"
)

// RONDB-1152: a parse error message quotes the request from the error
// onwards. Each REST thread parses in one reused buffer, so without a
// terminator after the request the quote ran on into the bytes of an earlier,
// longer request (another client's data), and past the buffer when no NUL
// followed. One keep-alive client sends both requests, so both are parsed by
// the same thread in the same buffer.
func TestParseErrorQuotesOnlyThisRequest(t *testing.T) {
	const marker = "EARLIER_REQUEST_BYTES_"
	client := testutils.SetupHttpClient(t)
	url := testutils.NewBatchReadURL()
	op := `{"method":"POST","relative-url":"db/t/` + config.PK_DB_OPERATION +
		`","body":{"filters":[{"column":"id","value":1}]}}`

	// Long and rejected while parsing: it only has to fill the buffer.
	long := `{"operations":[` + op + `,"` + strings.Repeat(marker, 200) + `"]}`
	testclient.SendHttpRequestWithClient(t, client, config.BATCH_HTTP_VERB, url,
		long, "", http.StatusBadRequest)

	for name, body := range map[string]string{
		"truncated":   `{"operations":[` + op + `,{"method":"POST","relative-url":"db`,
		"extra comma": `{"operations":[` + op + `,,]}`,
	} {
		_, resp := testclient.SendHttpRequestWithClient(t, client,
			config.BATCH_HTTP_VERB, url, body, "", http.StatusBadRequest)
		if strings.Contains(string(resp), marker) {
			t.Errorf("%s: the error message quotes an earlier request: %.300s",
				name, resp)
		}
	}
}
