package testclient

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	"hopsworks.ai/rdrs/internal/log"
	"hopsworks.ai/rdrs/internal/testutils"
	"hopsworks.ai/rdrs/pkg/api"
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

func NewReadColumns(prefix string, numReadColumns int) []api.ReadColumn {
	readColumns := make([]api.ReadColumn, numReadColumns)
	for i := 0; i < numReadColumns; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		drt := api.DRT_DEFAULT
		readColumns[i].Column = &col
		readColumns[i].DataReturnType = &drt
	}
	return readColumns
}

func NewReadColumn(col string) []api.ReadColumn {
	readColumns := make([]api.ReadColumn, 1)
	drt := string(api.DRT_DEFAULT)
	readColumns[0].Column = &col
	readColumns[0].DataReturnType = &drt
	return readColumns
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
func NewFilters(prefix string, numFilters int) []api.Filter {
	filters := make([]api.Filter, numFilters)
	for i := 0; i < numFilters; i++ {
		col := prefix + fmt.Sprintf("%d", i)
		val := col + "_data"
		v := rawBytes(val)
		filters[i] = api.Filter{Column: &col, Value: &v}
	}
	return filters
}

func NewFilter(column *string, a interface{}) []api.Filter {
	filter := make([]api.Filter, 1)

	filter[0] = api.Filter{Column: column}
	v := rawBytes(a)
	filter[0].Value = &v
	return filter
}

func NewFiltersKVs(vals ...interface{}) []api.Filter {
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
	return filters
}
