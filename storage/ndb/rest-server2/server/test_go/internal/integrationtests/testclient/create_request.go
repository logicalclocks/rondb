/*
 * Copyright (C) 2023, 2026 Hopsworks AB
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
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"hopsworks.ai/rdrs2/internal/log"
	"hopsworks.ai/rdrs2/internal/testutils"
	"hopsworks.ai/rdrs2/pkg/api"
)

func rawBytes(a interface{}) json.RawMessage {
	var value json.RawMessage
	if a == nil {
		return []byte("null")
	}

	switch v := a.(type) {
	case int8, int16, int32, int64, int, uint8, uint16, uint32, uint64, uint, float32, float64:
		value = []byte(fmt.Sprintf("%v", v))
	case string:
		value = []byte(strconv.Quote(v))
	default:
		panic(fmt.Errorf("unsupported data type. Type: %v", reflect.TypeOf(a)))
	}
	return value
}

func NewReadColumns(prefix string, numReadColumns int) *[]api.ReadColumn {
	readColumns := make([]api.ReadColumn, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		drt := api.DRT_DEFAULT
		readColumns[i].Column = &col
		readColumns[i].DataReturnType = &drt
	}
	return &readColumns
}

func NewReadColumn(col string) *[]api.ReadColumn {
	readColumns := make([]api.ReadColumn, 1)
	drt := string(api.DRT_DEFAULT)
	readColumns[0].Column = &col
	readColumns[0].DataReturnType = &drt
	return &readColumns
}

func NewOperationID(size int) *string {
	opID := testutils.RandString(size)
	return &opID
}

func NewPKReadReqBodyTBD() api.PKReadBody {
	param := api.PKReadBody{
		Filters:     NewFilters("filter_col_", 3),
		ReadColumns: NewReadColumns("read_col_", 5),
		OperationID: NewOperationID(64),
	}
	return param
}

// Creates dummy filter columns of type string
func NewFilters(prefix string, numFilters int) *[]api.Filter {
	filters := make([]api.Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		v := rawBytes(val)
		filters[i] = api.Filter{Column: &col, Value: &v}
	}
	return &filters
}

func NewFilter(column *string, a interface{}) *[]api.Filter {
	filter := make([]api.Filter, 1)

	filter[0] = api.Filter{Column: column}
	v := rawBytes(a)
	filter[0].Value = &v
	return &filter
}

func NewFiltersKVs(vals ...interface{}) *[]api.Filter {
	if len(vals)%2 != 0 {
		log.Panic("Expecting key value pairs")
	}

	filters := make([]api.Filter, len(vals)/2)
	fidx := 0
	for i := 0; i < len(vals); {
		c := fmt.Sprintf("%v", vals[i])
		v := rawBytes(vals[i+1])
		filters[fidx] = api.Filter{Column: &c, Value: &v}
		fidx++
		i += 2
	}
	return &filters
}

// NewWriteColumn creates a single write column
func NewWriteColumn(column string, value interface{}) *[]api.WriteColumn {
	writeColumns := make([]api.WriteColumn, 1)
	v := rawBytes(value)
	writeColumns[0] = api.WriteColumn{Column: &column, Value: &v}
	return &writeColumns
}

// NewWriteColumns creates multiple write columns with prefix and values
func NewWriteColumns(prefix string, numColumns int, value interface{}) *[]api.WriteColumn {
	writeColumns := make([]api.WriteColumn, numColumns)
	for i := 0; i < numColumns; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		v := rawBytes(value)
		writeColumns[i] = api.WriteColumn{Column: &col, Value: &v}
	}
	return &writeColumns
}

// NewWriteColumnsKVs creates write columns from key-value pairs
func NewWriteColumnsKVs(vals ...interface{}) *[]api.WriteColumn {
	if len(vals)%2 != 0 {
		log.Panic("Expecting key value pairs")
	}

	writeColumns := make([]api.WriteColumn, len(vals)/2)
	idx := 0
	for i := 0; i < len(vals); {
		c := fmt.Sprintf("%v", vals[i])
		v := rawBytes(vals[i+1])
		writeColumns[idx] = api.WriteColumn{Column: &c, Value: &v}
		idx++
		i += 2
	}
	return &writeColumns
}

// NewPKWriteReqBodyTBD creates a test PKWriteBody for testing
func NewPKWriteReqBodyTBD() api.PKWriteBody {
	param := api.PKWriteBody{
		Filters:      NewFilters("filter_col_", 1),
		WriteColumns: NewWriteColumnsKVs("col0", "test_value"),
		OperationID:  NewOperationID(64),
	}
	return param
}

// NewPKDeleteReqBodyTBD creates a test PKDeleteBody for testing
func NewPKDeleteReqBodyTBD() api.PKDeleteBody {
	param := api.PKDeleteBody{
		Filters:     NewFilters("filter_col_", 1),
		OperationID: NewOperationID(64),
	}
	return param
}
