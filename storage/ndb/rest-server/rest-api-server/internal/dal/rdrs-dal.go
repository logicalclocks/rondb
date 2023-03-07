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

package dal

/*
#cgo CFLAGS: -g -Wall
#include <stdlib.h>
#include "./../../../data-access-rondb/src/rdrs-dal.h"
#include "./../../../data-access-rondb/src/rdrs-hopsworks-dal.h"
#include "./../../../data-access-rondb/src/rdrs-const.h"
#include "./../../../data-access-rondb/src/error-strings.h"
*/
import "C"
import (
	"fmt"
	"net/http"
	"unsafe"
)

type DalError struct {
	HttpCode    int
	Message     string
	ErrLineNo   int
	ErrFileName string
}

func (e *DalError) Error() string {
	return e.Message
}

func (e *DalError) VerboseError() string {
	return fmt.Sprintf("%v; File: %v, Line: %v ", e.Message, e.ErrFileName, e.ErrLineNo)
}

type RonDBStats struct {
	NdbObjectsCreationCount int64
	NdbObjectsDeletionCount int64
	NdbObjectsTotalCount    int64
	NdbObjectsFreeCount     int64
}

func InitRonDBConnection(connStr string, find_available_node_id bool) *DalError {

	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.init(cs, C.uint(btoi(find_available_node_id)))

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func ShutdownConnection() *DalError {
	ret := C.shutdown_connection()

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}

func RonDBPKRead(request *NativeBuffer, response *NativeBuffer) *DalError {
	// unsafe.Pointer
	// create C structs for  buffers
	var crequest C.RS_Buffer
	var cresponse C.RS_Buffer
	crequest.buffer = (*C.char)(request.Buffer)
	crequest.size = C.uint(request.Size)

	cresponse.buffer = (*C.char)(response.Buffer)
	cresponse.size = C.uint(response.Size)

	ret := C.pk_read(&crequest, &cresponse)

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func RonDBBatchedPKRead(noOps uint32, requests []*NativeBuffer, responses []*NativeBuffer) *DalError {
	reqMem := C.malloc(C.size_t(noOps) * C.size_t(C.sizeof_RS_Buffer))
	defer C.free(reqMem)
	cReqs := unsafe.Slice((*C.RS_Buffer)(reqMem), noOps)

	respMem := C.malloc(C.size_t(noOps) * C.size_t(C.sizeof_RS_Buffer))
	defer C.free(respMem)
	cResps := unsafe.Slice((*C.RS_Buffer)(respMem), noOps)

	for i := 0; i < int(noOps); i++ {
		cReqs[i].buffer = (*C.char)(requests[i].Buffer)
		cReqs[i].size = C.uint(requests[i].Size)

		cResps[i].buffer = (*C.char)(responses[i].Buffer)
		cResps[i].size = C.uint(responses[i].Size)
	}

	ret := C.pk_batch_read(C.uint(noOps), (*C.RS_Buffer)(reqMem), (*C.RS_Buffer)(respMem))

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	return nil
}

func cToGoRet(ret *C.RS_Status) *DalError {
	return &DalError{HttpCode: int(ret.http_code), Message: C.GoString(&ret.message[0]),
		ErrLineNo: int(ret.err_line_no), ErrFileName: C.GoString(&ret.err_file_name[0])}
}

func GetRonDBStats() (*RonDBStats, *DalError) {

	p := (*C.RonDB_Stats)(C.malloc(C.size_t(C.sizeof_RonDB_Stats)))
	defer C.free(unsafe.Pointer(p))

	ret := C.get_rondb_stats(p)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}
	var rstats RonDBStats
	rstats.NdbObjectsCreationCount = int64(p.ndb_objects_created)
	rstats.NdbObjectsDeletionCount = int64(p.ndb_objects_deleted)
	rstats.NdbObjectsTotalCount = int64(p.ndb_objects_count)
	rstats.NdbObjectsFreeCount = int64(p.ndb_objects_available)

	return &rstats, nil
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
