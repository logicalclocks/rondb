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
	"encoding/base64"
	"net/http"
	"testing"

	// _ "github.com/ianlancetaylor/cgosymbolizer"
	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/integrationtests/testclient"
	"hopsworks.ai/rdrs/pkg/api"
	"hopsworks.ai/rdrs/resources/testdbs"
)

// INT TESTS
// Test signed and unsigned int data type
func TestDataTypesInt(t *testing.T) {
	testTable := "int_table"
	testDb := testdbs.DB004
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{
		"notfound": {
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", 100, "id1", 100),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusNotFound,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple1": {
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple2": { //with out operation ID
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple3": { //without read columns.
			PkReq:          api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", 0, "id1", 0)},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple4": { //Table with only primary keys
			PkReq: api.PKReadBody{Filters: testclient.NewFiltersKVs("id0", 0, "id1", 0),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          "int_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        []interface{}{},
		},

		"maxValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 2147483647, "id1", 4294967295),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"minValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -2147483648, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", -1), //id1 is unsigned
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        []interface{}{},
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 2147483648, "id1", 4294967295), // bigger than the range
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        []interface{}{},
		},

		"assigningSmallerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -2147483649, "id1", 0), //smaller than range
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        []interface{}{},
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
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

func TestDataTypesBigInt(t *testing.T) {

	testTable := "bigint_table"
	testDb := testdbs.DB005

	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"maxValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 9223372036854775807, "id1", uint64(18446744073709551615)),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"minValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -9223372036854775808, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 9223372036854775807, "id1", "18446744073709551616"), //18446744073709551615+1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "-9223372036854775809", "id1", 0), //-9223372036854775808-1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
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

func TestDataTypesTinyInt(t *testing.T) {

	testTable := "tinyint_table"
	testDb := testdbs.DB006
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"maxValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 127, "id1", 255),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"minValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -128, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 127, "id1", 256), //255+1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -129, "id1", 0), //-128-1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
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

func TestDataTypesSmallInt(t *testing.T) {

	testTable := "smallint_table"
	testDb := testdbs.DB007
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"maxValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 32767, "id1", 65535),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"minValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -32768, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 32768, "id1", 256), //32767+1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -32769, "id1", 0), //-32768-1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
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

func TestDataTypesMediumInt(t *testing.T) {
	testTable := "mediumint_table"
	testDb := testdbs.DB008
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"maxValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 8388607, "id1", 16777215),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"minValues": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -8388608, "id1", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0, "id1", -1), //id1 is unsigned
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 8388608, "id1", 256), //8388607+1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningSmallerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -8388609, "id1", 0), //-8388608-1
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1, "id1", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
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

func TestDataTypesFloat(t *testing.T) {

	// testTable := "float_table"
	testDb := testdbs.DB009
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"floatPK": { // NDB does not support floats PKs
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          "float_table2",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_017(),
		},

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          "float_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1"),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          "float_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 2),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          "float_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesDouble(t *testing.T) {

	// testTable := "float_table"
	testDb := testdbs.DB010
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"floatPK": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          "double_table2",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_017(),
		},

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 0),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          "double_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 1),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          "double_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", 2),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          "double_table1",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesDecimal(t *testing.T) {

	testTable := "decimal_table"
	testDb := testdbs.DB011
	validateColumns := []interface{}{"col0", "col1"}
	tests := map[string]api.PKTestInfo{

		"simple": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -12345.12345, "id1", 12345.12345),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nullVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -67890.12345, "id1", 67890.12345),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"assignNegativeValToUnsignedCol": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -12345.12345, "id1", -12345.12345),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(64),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"assigningBiggerVals": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", -12345.12345, "id1", 123456789.12345),
				ReadColumns: testclient.NewReadColumns("col", 2),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesBlobs(t *testing.T) {

	testDb := testdbs.DB013
	tests := map[string]api.PKTestInfo{

		"blob1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1"),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "blob_table",
			Db:             testDb,
			HttpCode:       http.StatusInternalServerError,
			ErrMsgContains: common.ERROR_026(),
			RespKVs:        []interface{}{},
		},

		"blob2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1"),
				ReadColumns: testclient.NewReadColumn("col1"),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "blob_table",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        []interface{}{"col1"},
		},

		"text1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1"),
				ReadColumns: testclient.NewReadColumns("col", 2),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "text_table",
			Db:             testDb,
			HttpCode:       http.StatusInternalServerError,
			ErrMsgContains: "",
			RespKVs:        []interface{}{},
		},

		"text2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1"),
				ReadColumns: testclient.NewReadColumn("col1"),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "text_table",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        []interface{}{"col1"},
		},
	}

	pkTestMultiple(t, tests, false)
}

func TestLargePks(t *testing.T) {
	testTable := "table_1"
	testDb := testdbs.DB026

	pkData := make([]byte, 3070)
	for i := 0; i < 3070; i++ {
		pkData[i] = 0x41
	}
	pkDataEncoded := base64.StdEncoding.EncodeToString(pkData)

	test := api.PKTestInfo{
		PkReq: api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id", pkDataEncoded),
			ReadColumns: testclient.NewReadColumns("col", 1),
			OperationID: testclient.NewOperationID(64),
		},
		Table:          testTable,
		Db:             testDb,
		HttpCode:       http.StatusOK,
		ErrMsgContains: "",
		RespKVs:        []interface{}{"col0"},
	}
	pkTest(t, test, true, true)
}

func TestLargeColumn(t *testing.T) {
	testTable := "table_1"
	testDb := testdbs.DB027

	test := api.PKTestInfo{
		PkReq: api.PKReadBody{
			Filters:     testclient.NewFiltersKVs("id", "1"),
			ReadColumns: testclient.NewReadColumns("col", 1),
			OperationID: testclient.NewOperationID(64),
		},
		Table:          testTable,
		Db:             testDb,
		HttpCode:       http.StatusOK,
		ErrMsgContains: "",
		RespKVs:        []interface{}{"col0"},
	}
	pkTest(t, test, true, true)
}

func TestDataTypesChar(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB012, false, 100, true)
}

func TestDataTypesVarchar(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB014, false, 50, false)
}

func TestDataTypesLongVarchar(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB015, false, 256, false)
}

func TestDataTypesBinary(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB016, true, 100, true)
}

func TestDataTypesVarbinary(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB017, true, 100, false)
}

func TestDataTypesLongVarbinary(t *testing.T) {
	ArrayColumnTest(t, "table1", testdbs.DB018, true, 256, false)
}

func ArrayColumnTest(t *testing.T, table string, database string, isBinary bool, colWidth int, padding bool) {
	t.Helper()
	testTable := table
	testDb := database
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"notfound1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("-1", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusNotFound,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"notfound2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue(*testclient.NewOperationID(colWidth*4 + 1), isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_008(),
			RespKVs:        validateColumns,
		},

		"simple1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("1", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("2", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple3": { // new line char in string
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("3", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple4": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("4", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"simple5": { //unicode pk
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("这是一个测验", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nulltest": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("5", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"escapedChars": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("6", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"quotedPK": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("\"7\"", isBinary, colWidth, padding)),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}

	pkTestMultiple(t, tests, isBinary)
}

func TestDataTypesDateColumn(t *testing.T) {
	t.Helper()
	testTable := "date_table"
	testDb := testdbs.DB019
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"validpk1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 00:00:00"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"invalidpk": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:00:00"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_008(),
			RespKVs:        []interface{}{},
		},

		"invalidpk2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 00:00:00.123123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_008(),
			RespKVs:        []interface{}{},
		},

		"nulltest1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-12"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"error": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-13-11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesDatetimeColumn(t *testing.T) {
	t.Helper()
	testDb := testdbs.DB020
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"validpk1_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11.123000"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 -11:11:11.123456"), //-iv sign should be ignored
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nulltest_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-12 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-12 11:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-12 11:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"wrongdate_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-13-11 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "date_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesTimeColumn(t *testing.T) {
	t.Helper()
	testDb := testdbs.DB021
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"validpk1_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:11:11.123000"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nulltest_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "12:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "12:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "12:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"wrongtime_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "11:61:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "time_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesTimestampColumn(t *testing.T) {
	t.Helper()
	testDb := testdbs.DB022
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"badts_1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1111-11-11 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},

		"badts_2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1970-01-01 00:00:00"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},

		"badts_3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2038-01-19 03:14:08"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},

		"validpk1_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 11:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"validpk1_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 11:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 11:11:11.123123"), // nanoseconds should be ignored
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 11:11:11.123000"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"validpk2_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-11 -11:11:11.123456"), //-iv sign should be ignored
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nulltest_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-12 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre3": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-12 11:11:11.123"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table3",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"nulltest_pre6": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-11-12 11:11:11.123456"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table6",
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"wrongdate_pre0": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022-13-11 11:11:11"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          "ts_table0",
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_027(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesYearColumn(t *testing.T) {
	///< Year 1901-2155 (1 byte)
	t.Helper()
	testDb := testdbs.DB023
	testTable := "year_table"
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{

		"simple1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2022"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"notfound1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1901"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusNotFound,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"notfound2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2155"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusNotFound,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"nulltest": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2023"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"baddate1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "1900"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},

		"baddate2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", "2156"),
				ReadColumns: testclient.NewReadColumns("col", 1),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusBadRequest,
			ErrMsgContains: common.ERROR_015(),
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, false)
}

func TestDataTypesBitColumn(t *testing.T) {
	t.Helper()
	testDb := testdbs.DB024
	testTable := "bit_table"
	validateColumns := []interface{}{"col0"}
	tests := map[string]api.PKTestInfo{
		"simple1": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("1", true, 100, true)),
				ReadColumns: testclient.NewReadColumns("col", 5),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
		"simple2": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("2", true, 100, true)),
				ReadColumns: testclient.NewReadColumns("col", 5),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},

		"null": {
			PkReq: api.PKReadBody{
				Filters:     testclient.NewFiltersKVs("id0", testclient.EncodePkValue("3", true, 100, true)),
				ReadColumns: testclient.NewReadColumns("col", 5),
				OperationID: testclient.NewOperationID(5),
			},
			Table:          testTable,
			Db:             testDb,
			HttpCode:       http.StatusOK,
			ErrMsgContains: "",
			RespKVs:        validateColumns,
		},
	}
	pkTestMultiple(t, tests, true)
}
