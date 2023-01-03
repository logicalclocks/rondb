/*
 * This file is part of the RonDB REST API Server
 * Copyright (c) 2022 Hopsworks AB
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
	"hopsworks.ai/rdrs/internal/integrationtests"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
)

func TestPKReadOmitRequired(t *testing.T) {
	integrationtests.WithDBs(t, []string{"DB000"},
		func(tc testutils.TlsContext) {
			// Test. Omitting filter should result in 400 error
			param := api.PKReadBody{
				Filters:     nil,
				ReadColumns: integrationtests.NewReadColumns("read_col_", 5),
				OperationID: integrationtests.NewOperationID(64),
			}

			url := integrationtests.NewPKReadURL("db", "table")

			body, _ := json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Error:Field validation for 'Filters'")

			// Test. unset filter values should result in 400 error
			col := "col"
			filter := integrationtests.NewFilter(&col, nil)
			param.Filters = filter
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Field validation for 'Value' failed on the 'required' tag")

			val := "val"
			filter = integrationtests.NewFilter(nil, val)
			param.Filters = filter
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"Field validation for 'Column' failed on the 'required' tag")
		})
}

func TestPKReadLargeColumns(t *testing.T) {
	integrationtests.WithDBs(t, []string{"DB000"},
		func(tc testutils.TlsContext) {

			// Test. Large filter column names.
			col := integrationtests.RandString(65)
			val := "val"
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, val),
				ReadColumns: integrationtests.NewReadColumns("read_col_", 5),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ := json.MarshalIndent(param, "", "\t")
			url := integrationtests.NewPKReadURL("db", "table")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body),
				http.StatusBadRequest, "Field validation for 'Column' failed on the 'max' tag")

			// Test. Large read column names.
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilters("filter_col_", 3),
				ReadColumns: integrationtests.NewReadColumns(integrationtests.RandString(65), 5),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB,
				url, string(body), http.StatusBadRequest, "field length validation failed")

			// Test. Large db and table names
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilters("filter_col_", 3),
				ReadColumns: integrationtests.NewReadColumns("read_col_", 5),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			url1 := integrationtests.NewPKReadURL(integrationtests.RandString(65), "table")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url1, string(body),
				http.StatusBadRequest, "Field validation for 'DB' failed on the 'max' tag")
			url2 := integrationtests.NewPKReadURL("db", integrationtests.RandString(65))
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url2, string(body),
				http.StatusBadRequest, "Field validation for 'Table' failed on the 'max' tag")
			url3 := integrationtests.NewPKReadURL("", "table")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url3, string(body),
				http.StatusBadRequest, "Field validation for 'DB' failed on the 'min' tag")
			url4 := integrationtests.NewPKReadURL("db", "")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url4, string(body), http.StatusBadRequest,
				"Field validation for 'Table' failed on the 'min' tag")
		})
}

func TestPKInvalidIdentifier(t *testing.T) {

	integrationtests.WithDBs(t, []string{"DB000"},
		func(tc testutils.TlsContext) {
			//Valid chars [ U+0001 .. U+007F] and [ U+0080 .. U+FFFF]
			// Test. invalid filter
			col := "col" + string(rune(0x0000))
			val := "val"
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, val),
				ReadColumns: integrationtests.NewReadColumn("read_col"),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ := json.MarshalIndent(param, "", "\t")
			url := integrationtests.NewPKReadURL("db", "table")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				fmt.Sprintf("field validation failed. Invalid character '%U' ", rune(0x0000)))

			// Test. invalid read col
			col = "col"
			val = "val"
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, val),
				ReadColumns: integrationtests.NewReadColumn("col" + string(rune(0x10000))),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))

			// Test. Invalid path parameteres
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, val),
				ReadColumns: integrationtests.NewReadColumn("col"),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			url1 := integrationtests.NewPKReadURL("db"+string(rune(0x10000)), "table")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url1, string(body), http.StatusBadRequest,
				fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))
			url2 := integrationtests.NewPKReadURL("db", "table"+string(rune(0x10000)))
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url2, string(body), http.StatusBadRequest,
				fmt.Sprintf("field validation failed. Invalid character '%U'", rune(0x10000)))
		})
}

func TestPKUniqueParams(t *testing.T) {

	integrationtests.WithDBs(t, []string{"DB000"},
		func(tc testutils.TlsContext) {
			// Test. unique read columns
			readColumns := make([]api.ReadColumn, 2)
			col := "col1"
			readColumns[0].Column = &col
			readColumns[1].Column = &col
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilters("col", 1),
				ReadColumns: &readColumns,
				OperationID: integrationtests.NewOperationID(64),
			}
			url := integrationtests.NewPKReadURL("db", "table")
			body, _ := json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"field validation for 'ReadColumns' failed on the 'unique' tag")

			// Test. unique filter columns
			col = "col"
			val := "val"
			filters := make([]api.Filter, 2)
			filters[0] = (*(integrationtests.NewFilter(&col, val)))[0]
			filters[1] = (*(integrationtests.NewFilter(&col, val)))[0]

			param = api.PKReadBody{
				Filters:     &filters,
				ReadColumns: integrationtests.NewReadColumns("read_col_", 5),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				"field validation for filter failed on the 'unique' tag")

			//Test that filter and read columns do not contain overlapping columns
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilter(&col, val),
				ReadColumns: integrationtests.NewReadColumn(col),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest,
				fmt.Sprintf("field validation for read columns faild. '%s' already included in filter", col))
		})
}

// DB/Table does not exist
func TestPKERROR_011(t *testing.T) {
	integrationtests.WithDBs(t, []string{"DB001"},
		func(tc testutils.TlsContext) {
			pkCol := "id0"
			pkVal := "1"
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilter(&pkCol, pkVal),
				ReadColumns: integrationtests.NewReadColumn("col_0"),
				OperationID: integrationtests.NewOperationID(64),
			}

			body, _ := json.MarshalIndent(param, "", "\t")

			url := integrationtests.NewPKReadURL("DB001_XXX", "table_1")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusUnauthorized, "")

			url = integrationtests.NewPKReadURL("DB001", "table_1_XXX")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_011())
		})
}

// column does not exist
func TestPKERROR_012(t *testing.T) {
	integrationtests.WithDBs(t, []string{"DB001"},
		func(tc testutils.TlsContext) {
			pkCol := "id0"
			pkVal := "1"
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilter(&pkCol, pkVal),
				ReadColumns: integrationtests.NewReadColumn("col_0_XXX"),
				OperationID: integrationtests.NewOperationID(64),
			}

			body, _ := json.MarshalIndent(param, "", "\t")

			url := integrationtests.NewPKReadURL("DB001", "table_1")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_012())
		})
}

// Primary key test.
func TestPKERROR_013_ERROR_014(t *testing.T) {
	integrationtests.WithDBs(t, []string{"DB002"},
		func(tc testutils.TlsContext) {
			// send an other request with one column missing from def
			// //		// one PK col is missing
			param := api.PKReadBody{
				Filters:     integrationtests.NewFilters("id", 1), // PK has two cols. should thow an exception as we have define only one col in PK
				ReadColumns: integrationtests.NewReadColumn("col_0"),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ := json.MarshalIndent(param, "", "\t")
			url := integrationtests.NewPKReadURL("DB002", "table_1")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_013())

			// send an other request with two pk cols but wrong names
			param = api.PKReadBody{
				Filters:     integrationtests.NewFilters("idx", 2),
				ReadColumns: integrationtests.NewReadColumn("col_0"),
				OperationID: integrationtests.NewOperationID(64),
			}
			body, _ = json.MarshalIndent(param, "", "\t")
			url = integrationtests.NewPKReadURL("DB002", "table_1")
			integrationtests.SendHttpRequest(t, tc, config.PK_HTTP_VERB, url, string(body), http.StatusBadRequest, common.ERROR_014())
		})
}
