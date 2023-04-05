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
 #include <stdlib.h>
 #include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"net/http"
	"unsafe"

	"hopsworks.ai/rdrs/internal/config"
	"hopsworks.ai/rdrs/internal/log"
)

func InitRonDBConnection(rondb config.RonDB) *DalError {
	log.Info("Initialising RonDB connection")

	cs := C.CString(rondb.GenerateMgmdConnectString())
	defer C.free(unsafe.Pointer(cs))

	nodeIDMem := C.malloc(C.size_t(len(rondb.NodeIDs)) * C.size_t(C.sizeof_uint))
	defer C.free(nodeIDMem)
	cNodeIDMem := unsafe.Slice((*C.uint)(nodeIDMem), len(rondb.NodeIDs))
	for i, node_id := range rondb.NodeIDs {
		cNodeIDMem[i] = C.uint(node_id)
	}

	ret := C.init(cs,
		C.uint(rondb.ConnectionPoolSize),
		(*C.uint)(nodeIDMem),
		C.uint(len(rondb.NodeIDs)),
		C.uint(rondb.ConnectionRetries),
		C.uint(rondb.ConnectionRetryDelayInSec))

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}

	// set failed ops retry properties
	return SetOpRetryProps(rondb.OpRetryOnTransientErrorsCount,
		rondb.OpRetryInitialDelayInMS, rondb.OpRetryJitterInMS)
}

func SetOpRetryProps(opRetryOnTransientErrorsCount, opRetryInitialDelayInMS uint32, jitter uint32) *DalError {
	ret := C.set_op_retry_props(C.uint(opRetryOnTransientErrorsCount),
		C.uint(opRetryInitialDelayInMS), C.uint(jitter))
	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}

func ShutdownConnection() *DalError {
	log.Info("Shutting down RonDB connection")
	ret := C.shutdown_connection()

	if ret.http_code != http.StatusOK {
		return cToGoRet(&ret)
	}
	return nil
}
