/*
 * Copyright (c) 2023, 2025, Hopsworks and/or its affiliates.
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

package main

/*
#include <string.h>
#include "../src/error_strings.h"
*/
import "C"
import (
	"fmt"
	"os"
	"reflect"
	"sync/atomic"
	"unsafe"

	"github.com/bytedance/sonic"
	"github.com/hamba/avro/v2"
)

var curSchemaID atomic.Int64

type ComplexFeature struct {
	schemaStr  string
	AvroSchema *avro.Schema
	AvroStruct *reflect.Type
}

var avroStructs = make(map[int64]*ComplexFeature)

//export register_schema
func register_schema(schema string, outSchemaID *C.int64_t) C.ErrorCode {

	avroSchema, err := avro.Parse(string(schema))
	if err != nil {
		return C.ERROR_AVRO_SCHEMA_PARSE_FAIL
	}

	avroStruct, err := ConvertAvroSchemaToStruct(avroSchema)
	if err != nil {
		return C.ERROR_AVRO_STRUCT_CREATION_FAILED
	}

	id := curSchemaID.Add(1)
	avroStructs[id] = &ComplexFeature{schemaStr: schema, AvroSchema: &avroSchema, AvroStruct: &avroStruct}
	*outSchemaID = C.int64_t(id)

	return C.NO_ERROR
}

//export unregister_schema
func unregister_schema(schema_id C.int64_t) {
	delete(avroStructs, int64(schema_id))
}

//export unmarshal_avro
func unmarshal_avro(schema_id C.int64_t, data []byte, outStr **C.char, outLen *C.int32_t) C.ErrorCode {

	cf, ok := avroStructs[int64(schema_id)]
	if !ok {
		return C.ERROR_AVRO_SCHEMA_STRUCT_NOT_FOUND
	}

	avroDeserialized := reflect.New(*cf.AvroStruct).Interface()
	err := avro.Unmarshal(*cf.AvroSchema, data, &avroDeserialized)
	if err != nil {
		return C.ERROR_AVRO_UNMARSHAL_FAILED
	}

	// dicsard the top most wrapper
	j := reflect.ValueOf(avroDeserialized).Elem().Field(0).Interface()

	bytes, err := sonic.Marshal(j)
	if err != nil {
		return C.ERROR_AVRO_JSON_CREATION_FAILED
	}

	heapStr := C.malloc(C.size_t(len(bytes)))
	if heapStr == nil {
		fmt.Fprintln(os.Stderr, "Failed to allocate memory\n")
		return C.ERROR_MEMORY_ALLOCATION_FAILURE
	}

	// Copy Go slice to C heap memory
	C.memcpy(heapStr, unsafe.Pointer(&bytes[0]), C.size_t(len(bytes)))

	*outStr = (*C.char)(heapStr)
	*outLen = C.int32_t(len(bytes))

	return C.NO_ERROR
}

func main() {}
