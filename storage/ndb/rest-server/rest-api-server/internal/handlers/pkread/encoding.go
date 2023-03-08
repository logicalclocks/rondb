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

/*
 #include "./../../../../data-access-rondb/src/rdrs-const.h"
 #include "./../../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"unsafe"

	"hopsworks.ai/rdrs/internal/common"
	"hopsworks.ai/rdrs/internal/dal/heap"
	"hopsworks.ai/rdrs/pkg/api"
)

// Also checkout internal/router/handler/pkread/encoding-scheme.png

/*
	 PK READ Request
	 ===============

	 HEADER
	 ======
	 [   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ][   4B   ] ....
	 Type     Capacity  Length     DB         Table      PK     Read Cols    Op_ID    TX_ID
								 Offset      Offset    Offset     Offset     Offset   Offset
	 BODY
	 ====
	 [ bytes ... ]
	 Null terminated DB Name

	 [ bytes ... ]
	 Null terminated Table Name

	 [   4B   ][   4B   ]...[   4B   ][   4B   ][   4B   ][   bytes ...  ][ 2B ] [ bytes... ][   4B   ][   4B   ] ....
	 Count     kv 1          kv n       key       value     key          val     val
			 offset        offset     offset     offset                 size
										 ^
				 ________________________|                                                     ^
							 _________________________________________________________________|


	 [   4B   ] [  4B     ] [  4B     ] ...
	 Count   col1 offset   col2 offset

	 [  4B ] [   bytes ... ] [  4B ] [   bytes ... ] ...
	 type     null terminated column names

	 [ bytes ... ] ...
	 null terminated  operation Id
*/

func CreateNativeRequest(
	pkrParams *api.PKReadParams,
	request, response *heap.NativeBuffer,
) (err error) {
	iBuf := unsafe.Slice((*uint32)(request.Buffer), request.Size/C.ADDRESS_SIZE)

	// First N bytes are for header
	var head uint32 = C.PK_REQ_HEADER_END

	dbOffSet := head

	head, err = common.CopyGoStrToCStr([]byte(*pkrParams.DB), request, head)
	if err != nil {
		return
	}

	tableOffSet := head
	head, err = common.CopyGoStrToCStr([]byte(*pkrParams.Table), request, head)
	if err != nil {
		return
	}

	// PK Filters
	head = common.AlignWord(head)
	pkOffset := head
	iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.Filters))
	head += C.ADDRESS_SIZE

	kvi := head / C.ADDRESS_SIZE // index for storing offsets for each key/value pair
	// skip for N number of offsets one for each key/value pair
	head = head + (uint32(len(*pkrParams.Filters)) * C.ADDRESS_SIZE)
	for _, filter := range *pkrParams.Filters {
		head = common.AlignWord(head)

		tupleOffset := head

		head = head + 8 //  for key and value offsets
		keyOffset := head
		head, err = common.CopyGoStrToCStr([]byte(*filter.Column), request, head)
		if err != nil {
			return
		}
		valueOffset := head
		head, err = common.CopyGoStrToNDBStr(*filter.Value, request, head)
		if err != nil {
			return
		}

		iBuf[kvi] = tupleOffset
		kvi++
		iBuf[tupleOffset/C.ADDRESS_SIZE] = keyOffset
		iBuf[(tupleOffset/C.ADDRESS_SIZE)+1] = valueOffset
	}

	// Read Columns
	head = common.AlignWord(head)
	var readColsOffset uint32 = 0
	if pkrParams.ReadColumns != nil {
		readColsOffset = head
		iBuf[head/C.ADDRESS_SIZE] = uint32(len(*pkrParams.ReadColumns))
		head += C.ADDRESS_SIZE

		rci := head / C.ADDRESS_SIZE // index for storing ofsets for each read column
		// skip for N number of offsets one for each column name
		head = head + (uint32(len(*pkrParams.ReadColumns)) * C.ADDRESS_SIZE)

		for _, col := range *pkrParams.ReadColumns {
			head = common.AlignWord(head)

			iBuf[rci] = head
			rci++

			// return type
			var drt uint32 = C.DEFAULT_DRT
			if col.DataReturnType != nil {
				drt, err = dataReturnType(col.DataReturnType)
				if err != nil {
					return
				}
			}

			iBuf[head/C.ADDRESS_SIZE] = drt
			head += C.ADDRESS_SIZE

			// col name
			head, err = common.CopyGoStrToCStr([]byte(*col.Column), request, head)
			if err != nil {
				return
			}
		}
	}

	// Operation ID
	var opIdOffset uint32 = 0
	if pkrParams.OperationID != nil {
		opIdOffset = head
		head, err = common.CopyGoStrToCStr([]byte(*pkrParams.OperationID), request, head)
		if err != nil {
			return
		}
	}

	// request buffer header
	iBuf[C.PK_REQ_OP_TYPE_IDX] = uint32(C.RDRS_PK_REQ_ID)
	iBuf[C.PK_REQ_CAPACITY_IDX] = uint32(request.Size)
	iBuf[C.PK_REQ_LENGTH_IDX] = uint32(head)
	iBuf[C.PK_REQ_DB_IDX] = uint32(dbOffSet)
	iBuf[C.PK_REQ_TABLE_IDX] = uint32(tableOffSet)
	iBuf[C.PK_REQ_PK_COLS_IDX] = uint32(pkOffset)
	iBuf[C.PK_REQ_READ_COLS_IDX] = uint32(readColsOffset)
	iBuf[C.PK_REQ_OP_ID_IDX] = uint32(opIdOffset)

	//xxd.Print(0, bBuf[:])
	return
}

func ProcessPKReadResponse(respBuff *heap.NativeBuffer, response api.PKReadResponse) (int32, error) {
	iBuf := unsafe.Slice((*uint32)(respBuff.Buffer), respBuff.Size)

	responseType := iBuf[C.PK_RESP_OP_TYPE_IDX]
	if responseType != C.RDRS_PK_RESP_ID {
		return http.StatusInternalServerError, errors.New("wrong response type")
	}

	// some sanity checks
	capacity := iBuf[C.PK_RESP_CAPACITY_IDX]
	dataLength := iBuf[C.PK_RESP_LENGTH_IDX]
	if respBuff.Size != capacity || !(dataLength < capacity) {
		return http.StatusInternalServerError,
			fmt.Errorf("response buffer may be corrupt. Buffer capacity: %d, Buffer data length: %d", capacity, dataLength)
	}

	opIDX := iBuf[C.PK_RESP_OP_ID_IDX]
	if opIDX != 0 {
		goOpID := C.GoString((*C.char)(unsafe.Pointer(uintptr(respBuff.Buffer) + uintptr(opIDX))))
		response.SetOperationID(&goOpID)
	}

	status := int32(iBuf[C.PK_RESP_OP_STATUS_IDX])
	if status == http.StatusOK { //
		colIDX := iBuf[C.PK_RESP_COLS_IDX]
		colCount := *(*uint32)(unsafe.Pointer(uintptr(respBuff.Buffer) + uintptr(colIDX)))

		for i := uint32(0); i < colCount; i++ {
			colHeaderStart := (*uint32)(unsafe.Pointer(
				uintptr(respBuff.Buffer) +
					uintptr(colIDX+
						uint32(C.ADDRESS_SIZE)+ // +1 for skipping the column count
						(i*4*C.ADDRESS_SIZE)))) // 4 number of header fieldse

			colHeader := unsafe.Slice((*uint32)(colHeaderStart), 4)

			nameAdd := colHeader[0]
			name := C.GoString((*C.char)(unsafe.Pointer(uintptr(respBuff.Buffer) + uintptr(nameAdd))))

			valueAdd := colHeader[1]

			isNull := colHeader[2]
			dataType := colHeader[3]

			if isNull == 0 {
				value := C.GoString((*C.char)(unsafe.Pointer(uintptr(respBuff.Buffer) + uintptr(valueAdd))))
				response.SetColumnData(&name, &value, dataType)
			} else {
				response.SetColumnData(&name, nil, dataType)
			}
		}
	}

	return status, nil
}

func convertToJsonRaw(dataType uint32, value *string) *json.RawMessage {
	if dataType == C.RDRS_INTEGER_DATATYPE || dataType == C.RDRS_FLOAT_DATATYPE {
		valueBytes := json.RawMessage(*value)
		return &valueBytes
	} else {
		quotedString := fmt.Sprintf("\"%s\"", *value)
		valueBytes := json.RawMessage(quotedString)
		return &valueBytes
	}
}

func dataReturnType(drt *string) (uint32, error) {
	if *drt == api.DRT_DEFAULT {
		return C.DEFAULT_DRT, nil
	} else {
		return math.MaxUint32, fmt.Errorf("return data type is not supported. Data type: %s", *drt)
	}
}
