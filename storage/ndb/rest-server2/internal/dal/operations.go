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

package dal

/*
 #cgo CFLAGS: -g -Wall
 #include <stdlib.h>
 #include "./../../server/src/rdrs_dal.h"
 #include "./../../server/src/rdrs_hopsworks_dal.h"
 #include "./../../server/src/rdrs_const.h"
 #include "./../../server/src/error_strings.h"
*/
import "C"
import (
	"net/http"
	"unsafe"

	"hopsworks.ai/rdrs2/internal/dal/heap"
)

type RonDBStats struct {
	NdbObjectsCreationCount int64
	NdbObjectsDeletionCount int64
	NdbObjectsTotalCount    int64
	NdbObjectsFreeCount     int64
	NdbConnectionState      int64
}

func RonDBPKRead(request *heap.NativeBuffer, response *heap.NativeBuffer) *DalError {
	// unsafe.Pointer
	// create C structs for buffers
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

func RonDBBatchedPKRead(noOps uint32, requests []*heap.NativeBuffer, responses []*heap.NativeBuffer) *DalError {
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
	rstats.NdbConnectionState = int64(p.connection_state)
	return &rstats, nil
}
