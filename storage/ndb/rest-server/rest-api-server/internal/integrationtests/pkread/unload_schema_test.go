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
	"net/http"
	"testing"

	// _ "github.com/ianlancetaylor/cgosymbolizer"

	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

func TestUnloadSchema(t *testing.T) {

	err := testutils.RunQueriesOnDataCluster(testdbs.DB025Scheme)
	if err != nil {
		t.Fatalf("failed to re-create database. Error: %v", err)
	}

	testTable := "table_1"
	testDb := testdbs.DB025
	validateColumns := []interface{}{"col0", "col1", "col2"}
	tests := map[string]api.PKTestInfo{
		"simple": {
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", "1"),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}

	pkTestMultiple(t, tests, false)

	err = testutils.RunQueriesOnDataCluster(testdbs.DB025UpdateScheme)
	if err != nil {
		t.Fatalf("failed to re-create database. Error: %v", err)
	}

	validateColumns = []interface{}{"new_col0", "new_col1", "new_col2"}
	tests = map[string]api.PKTestInfo{
		"simple": {
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", "1"),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}
