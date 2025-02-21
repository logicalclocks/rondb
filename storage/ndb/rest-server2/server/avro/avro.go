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
#include <stdint.h>
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
func register_schema(schema string) C.int64_t {

	avroSchema, err := avro.Parse(string(schema))
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Failed to parse avro schemd %s. Error: %v\n", schema, err)
		return -1
	}

	avroStruct, err := ConvertAvroSchemaToStruct(avroSchema)
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Failed to generate strcut for avro schemd %s. Error: %v\n", schema, err)
		return -1
	}
	id := curSchemaID.Add(1)
	avroStructs[id] = &ComplexFeature{schemaStr: schema, AvroSchema: &avroSchema, AvroStruct: &avroStruct}

	//fmt.Printf("Go lang. Registered schema: %s. ID: %d\n", schema, id)
	return C.int64_t(id)
}

//export unregister_schema
func unregister_schema(schema_id C.int64_t) {
	delete(avroStructs, int64(schema_id))
	//fmt.Printf("Go lang. Deleted schema ID: %d\n", schema_id)
}

//export unmarshal_avro
func unmarshal_avro(schema_id C.int64_t, data []byte, outStr **C.char, outLen *C.int32_t) C.int64_t {
	//fmt.Printf("Go lang. Unmarshal: Schema ID:  %d\n", schema_id)

	// var avroDeserialized interface{}
	cf, ok := avroStructs[int64(schema_id)]
	if !ok {
		//fmt.Fprintf(os.Stderr, "Failed to unmarshall avro data. Schema ID: %d not found \n", schema_id)
		return C.int64_t(-1)
	}

	avroDeserialized := reflect.New(*cf.AvroStruct).Interface()
	err := avro.Unmarshal(*cf.AvroSchema, data, &avroDeserialized)
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Failed to unmarshall avro data. Schema ID: %d. Error: %v\n", schema_id, err)
		return C.int64_t(-1)
	}

	// dicsard the top most wrapper
	j := reflect.ValueOf(avroDeserialized).Elem().Field(0).Interface()

	bytes, err := sonic.Marshal(j)
	if err != nil {
		//fmt.Fprintf(os.Stderr, "Failed to unmarshall avro data. Schema ID: %d. Error: %v\n", schema_id, err)
		return C.int64_t(-1)
	}

	heapStr := C.malloc(C.size_t(len(bytes)))
	if heapStr == nil {
		fmt.Fprintln(os.Stderr, "Failed to allocate memory\n")
		return C.int64_t(-1)
	}

	// Copy Go slice to C heap memory
	C.memcpy(heapStr, unsafe.Pointer(&bytes[0]), C.size_t(len(bytes)))

	*outStr = (*C.char)(heapStr)
	*outLen = C.int32_t(len(bytes))

	return C.int64_t(0)
}

func main() {}
