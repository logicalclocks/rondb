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
 #include <stdlib.h>
 #include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"net/http"
	"unsafe"

	"hopsworks.ai/rdrs/internal/log"
)

func InitRonDBConnection(connStr string, find_available_node_id bool) *DalError {
	log.Info("Initialising RonDB connection")
	cs := C.CString(connStr)
	defer C.free(unsafe.Pointer(cs))
	ret := C.init(cs, C.uint(btoi(find_available_node_id)))

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
