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
 #include "./../../../data-access-rondb/src/rdrs-hopsworks-dal.h"
 #include "./../../../data-access-rondb/src/rdrs-dal.h"
*/
import "C"
import (
	"net/http"
	"unsafe"
)

type HopsworksAPIKey struct {
	Secret string
	Salt   string
	Name   string
	UserID int
}

func GetAPIKey(userKey string) (*HopsworksAPIKey, *DalError) {
	cUserKey := C.CString(userKey)
	defer C.free(unsafe.Pointer(cUserKey))

	apiKey := (*C.HopsworksAPIKey)(C.malloc(C.size_t(C.sizeof_HopsworksAPIKey)))
	defer C.free(unsafe.Pointer(apiKey))

	ret := C.find_api_key(cUserKey, apiKey)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}

	hopsworksAPIKey := HopsworksAPIKey{
		Secret: C.GoString(&apiKey.secret[0]),
		Salt:   C.GoString(&apiKey.salt[0]),
		Name:   C.GoString(&apiKey.name[0]),
		UserID: int(apiKey.user_id),
	}

	return &hopsworksAPIKey, nil
}

func GetUserProjects(uid int) ([]string, *DalError) {
	var dbs []string

	var count C.int
	countptr := (*C.int)(unsafe.Pointer(&count))

	var projects **C.char
	projectsPtr := (***C.char)(unsafe.Pointer(&projects))

	ret := C.find_all_projects(C.int(uid), projectsPtr, countptr)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}

	dstBuf := unsafe.Slice((**C.char)(unsafe.Pointer(projects)), count)

	for _, buff := range dstBuf {
		db := C.GoString(buff)
		dbs = append(dbs, db)
		C.free(unsafe.Pointer(buff))
	}
	C.free(unsafe.Pointer(projects))

	return dbs, nil
}
