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
 #include "./../../../data-access-rondb/src/feature_store/feature_store.h"
*/
import "C"
import (
	"net/http"
	"unsafe"
)

func GetProjectID(featureStoreName string) (int, *DalError) {
	cFeatureStoreName := C.CString(featureStoreName)
	defer C.free(unsafe.Pointer(cFeatureStoreName))

	var projectID C.int
	projectIDPtr := (*C.int)(unsafe.Pointer(&projectID))

	ret := C.find_project_id(cFeatureStoreName, projectIDPtr)

	if ret.http_code != http.StatusOK {
		return 0, cToGoRet(&ret)
	}

	return int(projectID), nil
}

func GetFeatureStorID(featureStoreName string) (int, *DalError) {
	cFeatureStoreName := C.CString(featureStoreName)
	defer C.free(unsafe.Pointer(cFeatureStoreName))

	var projectID C.int
	projectIDPtr := (*C.int)(unsafe.Pointer(&projectID))

	ret := C.find_feature_store_id(cFeatureStoreName, projectIDPtr)

	if ret.http_code != http.StatusOK {
		return 0, cToGoRet(&ret)
	}

	return int(projectID), nil
}

func GetFeatureViewID(featureStoreID int, featureViewName string, featureViewVersion int) (int, *DalError) {
	cFeatureViewName := C.CString(featureViewName)
	defer C.free(unsafe.Pointer(cFeatureViewName))

	var fsViewID C.int
	fsViewIDPtr := (*C.int)(unsafe.Pointer(&fsViewID))

	ret := C.find_feature_view_id(C.int(featureStoreID),
		cFeatureViewName,
		C.int(featureViewVersion),
		fsViewIDPtr)

	if ret.http_code != http.StatusOK {
		return 0, cToGoRet(&ret)
	}

	return int(fsViewID), nil
}
