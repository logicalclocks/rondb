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

package pkread

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestPKReadOmitRequired(t *testing.T) {
	// Test. Omitting filter should result in 400 error
	param := api.PKReadBody{
		Filters:     nil,
		ReadColumns: testclient.NewReadColumns("read_col_", 5),
		OperationID: testclient.NewOperationID(64),
	}

	url := testutils.NewPKReadURL("db", "table")

	body, _ := json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"Error:Field validation for 'Filters'", http.StatusBadRequest)

	// Test. unset filter values should result in 400 error
	col := "col"
	filter := testclient.NewFilter(&col, nil)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"Field validation for 'Value' failed on the 'required' tag", http.StatusBadRequest)

	val := "val"
	filter = testclient.NewFilter(nil, val)
	param.Filters = filter
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"Field validation for 'Column' failed on the 'required' tag", http.StatusBadRequest)
}

func TestPKReadLargeColumns(t *testing.T) {
	// Test. Large filter column names.
	col := testutils.RandString(65)
	val := "val"
	param := api.PKReadBody{
		Filters:     testclient.NewFilter(&col, val),
		ReadColumns: testclient.NewReadColumns("read_col_", 5),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := testutils.NewPKReadURL("db", "table")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"Field validation for 'Column' failed on the 'max' tag", http.StatusBadRequest)

	// Test. Large read column names.
	param = api.PKReadBody{
		Filters:     testclient.NewFilters("filter_col_", 3),
		ReadColumns: testclient.NewReadColumns(testutils.RandString(65), 5),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB,
		url, string(body), "identifier is too large", http.StatusBadRequest)

	// Test. Large db and table names
	param = api.PKReadBody{
		Filters:     testclient.NewFilters("filter_col_", 3),
		ReadColumns: testclient.NewReadColumns("read_col_", 5),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := testutils.NewPKReadURL(testutils.RandString(65), "table")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url1, string(body),
		"Field validation for 'DB' failed on the 'max' tag", http.StatusBadRequest)
	url2 := testutils.NewPKReadURL("db", testutils.RandString(65))
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url2, string(body),
		"Field validation for 'Table' failed on the 'max' tag", http.StatusBadRequest)
	url3 := testutils.NewPKReadURL("", "table")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url3, string(body),
		"Field validation for 'DB' failed on the 'min' tag", http.StatusBadRequest)
	url4 := testutils.NewPKReadURL("db", "")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url4, string(body),
		"Field validation for 'Table' failed on the 'min' tag", http.StatusBadRequest)
}

func TestPKInvalidIdentifier(t *testing.T) {
	//Valid chars [ U+0001 .. U+007F] and [ U+0080 .. U+FFFF]
	// Test. invalid filter
	col := "col" + string(rune(0x0000))
	val := "val"
	param := api.PKReadBody{
		Filters:     testclient.NewFilter(&col, val),
		ReadColumns: testclient.NewReadColumn("read_col"),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := testutils.NewPKReadURL("db", "table")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		fmt.Sprintf("invalid character '%U' ", rune(0x0000)), http.StatusBadRequest)

	// Test. invalid read col
	col = "col"
	val = "val"
	param = api.PKReadBody{
		Filters:     testclient.NewFilter(&col, val),
		ReadColumns: testclient.NewReadColumn("col" + string(rune(0x10000))),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		fmt.Sprintf("invalid character '%U'", rune(0x10000)), http.StatusBadRequest)

	// Test. Invalid path parameteres
	param = api.PKReadBody{
		Filters:     testclient.NewFilter(&col, val),
		ReadColumns: testclient.NewReadColumn("col"),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url1 := testutils.NewPKReadURL("db"+string(rune(0x10000)), "table")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url1, string(body),
		fmt.Sprintf("invalid character '%U'", rune(0x10000)), http.StatusBadRequest)
	url2 := testutils.NewPKReadURL("db", "table"+string(rune(0x10000)))
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url2, string(body),
		fmt.Sprintf("invalid character '%U'", rune(0x10000)), http.StatusBadRequest)
}

func TestPKUniqueParams(t *testing.T) {
	// Test. unique read columns
	readColumns := make([]api.ReadColumn, 2)
	col := "col1"
	readColumns[0].Column = &col
	readColumns[1].Column = &col
	param := api.PKReadBody{
		Filters:     testclient.NewFilters("col", 1),
		ReadColumns: readColumns,
		OperationID: testclient.NewOperationID(64),
	}
	url := testutils.NewPKReadURL("db", "table")
	body, _ := json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"field validation for 'ReadColumns' failed on the 'unique' tag", http.StatusBadRequest)

	// Test. unique filter columns
	col = "col"
	val := "val"
	filters := make([]api.Filter, 2)
	filters[0] = (testclient.NewFilter(&col, val))[0]
	filters[1] = (testclient.NewFilter(&col, val))[0]

	param = api.PKReadBody{
		Filters:     filters,
		ReadColumns: testclient.NewReadColumns("read_col_", 5),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"field validation for filter failed on the 'unique' tag", http.StatusBadRequest)

	//Test that filter and read columns do not contain overlapping columns
	param = api.PKReadBody{
		Filters:     testclient.NewFilter(&col, val),
		ReadColumns: testclient.NewReadColumn(col),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		fmt.Sprintf("field validation for read columns faild. '%s' already included in filter", col),
		http.StatusBadRequest)
}

// DB/Table does not exist
func TestPKERROR_011(t *testing.T) {
	pkCol := "id0"
	pkVal := "1"
	param := api.PKReadBody{
		Filters:     testclient.NewFilter(&pkCol, pkVal),
		ReadColumns: testclient.NewReadColumn("col0"),
		OperationID: testclient.NewOperationID(64),
	}

	body, _ := json.MarshalIndent(param, "", "\t")

	url := testutils.NewPKReadURL("DB001_XXX", "table_1")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		"", http.StatusUnauthorized)

	url = testutils.NewPKReadURL(testdbs.DB001, "table_1_XXX")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		common.ERROR_011(), http.StatusNotFound)
}

// column does not exist
func TestPKERROR_012(t *testing.T) {
	pkCol := "id0"
	pkVal := "1"
	param := api.PKReadBody{
		Filters:     testclient.NewFilter(&pkCol, pkVal),
		ReadColumns: testclient.NewReadColumn("col0_XXX"),
		OperationID: testclient.NewOperationID(64),
	}

	body, _ := json.MarshalIndent(param, "", "\t")

	url := testutils.NewPKReadURL(testdbs.DB001, "table_1")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		common.ERROR_012(), http.StatusBadRequest)
}

// Primary key test.
func TestPKERROR_013_ERROR_014(t *testing.T) {
	// send an other request with one column missing from def
	// //		// one PK col is missing
	param := api.PKReadBody{
		Filters:     testclient.NewFilters("id", 1), // PK has two cols. should thow an exception as we have define only one col in PK
		ReadColumns: testclient.NewReadColumn("col0"),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ := json.MarshalIndent(param, "", "\t")
	url := testutils.NewPKReadURL(testdbs.DB002, "table_1")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		common.ERROR_013(), http.StatusBadRequest)

	// send an other request with two pk cols but wrong names
	param = api.PKReadBody{
		Filters:     testclient.NewFilters("idx", 2),
		ReadColumns: testclient.NewReadColumn("col0"),
		OperationID: testclient.NewOperationID(64),
	}
	body, _ = json.MarshalIndent(param, "", "\t")
	url = testutils.NewPKReadURL(testdbs.DB002, "table_1")
	testclient.SendHttpRequest(t, config.PK_HTTP_VERB, url, string(body),
		common.ERROR_014(), http.StatusBadRequest)
}
