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
 #include "./../../../data-access-rondb/src/rdrs-const.h"
 #include "./../../../data-access-rondb/src/feature_store/feature_store.h"
*/
import "C"
import (
	"net/http"
	"sort"
	"unsafe"

	"hopsworks.ai/rdrs/internal/log"
)

type TrainingDatasetFeature struct {
	FeatureID                int
	TrainingDataset          int
	FeatureGroupID           int
	Name                     string
	Type                     string
	TDJoinID                 int
	IDX                      int
	Label                    int
	TransformationFunctionID int
	FeatureViewID            int
}

type TrainingDatasetJoin struct {
	Id     int
	Prefix string
}

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

func GetFeatureStoreID(featureStoreName string) (int, *DalError) {
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

func GetTrainingDatasetJoinData(featureViewID int) ([]TrainingDatasetJoin, *DalError) {
	var tdjsSize C.int
	tdjsSizePtr := (*C.int)(unsafe.Pointer(&tdjsSize))

	var tdjs *C.Training_Dataset_Join
	tdjsPtr := (**C.Training_Dataset_Join)(unsafe.Pointer(&tdjs))

	ret := C.find_training_dataset_join_data(C.int(featureViewID), tdjsPtr, tdjsSizePtr)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}

	tdjsSlice := unsafe.Slice((*C.Training_Dataset_Join)(unsafe.Pointer(tdjs)), tdjsSize)

	retTdjs := make([]TrainingDatasetJoin, int(tdjsSize))
	for i, tdj := range tdjsSlice {
		retTdj := TrainingDatasetJoin{
			Id:     int(tdj.id),
			Prefix: C.GoString(&tdj.prefix[0]),
		}
		retTdjs[i] = retTdj
	}

	C.free(unsafe.Pointer(tdjs))
	return retTdjs[:], nil
}

type FeatureGroup struct {
	Name           string
	FeatureStoreId int
	Version        int
	OnlineEnabled  bool
	NumOfPk        int
	PrimaryKey     []string
}

func GetFeatureGroupData(featureGroupID int) (*FeatureGroup, *DalError) {

	var fg C.Feature_Group
	fgPtr := (*C.Feature_Group)(unsafe.Pointer(&fg))

	ret := C.find_feature_group_data(C.int(featureGroupID), fgPtr)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}

	var n_pk = int(fg.num_pk)
	var pks = make([]string, n_pk)
	pkSlice := unsafe.Slice((**C.char)(unsafe.Pointer(fg.primary_key)), n_pk)
	for i, buff := range pkSlice {
		pk := C.GoString(buff)
		pks[i] = pk
		C.free(unsafe.Pointer(buff))
	}
	C.free(unsafe.Pointer(fg.primary_key))

	if log.IsDebug() {
		log.Debugf("Size of pk is %d", n_pk)
		for _, pk := range(pks) {
			log.Debugf("primary key: %s", pk)
		}
	}
	var fgGo = FeatureGroup{
		Name:           C.GoString(&fg.name[0]),
		FeatureStoreId: int(fg.feature_store_id),
		Version:        int(fg.version),
		OnlineEnabled:  int(fg.online_enabled) != 0,
		PrimaryKey:     pks,
	}
	return &fgGo, nil
}

func GetTrainingDatasetFeature(featureViewID int) ([]TrainingDatasetFeature, *DalError) {

	var tdfsSize C.int
	tdfsSizePtr := (*C.int)(unsafe.Pointer(&tdfsSize))

	var tdfs *C.Training_Dataset_Feature
	tdfsPtr := (**C.Training_Dataset_Feature)(unsafe.Pointer(&tdfs))

	ret := C.find_training_dataset_data(C.int(featureViewID), tdfsPtr, tdfsSizePtr)

	if ret.http_code != http.StatusOK {
		return nil, cToGoRet(&ret)
	}

	tdfsSlice := unsafe.Slice((*C.Training_Dataset_Feature)(unsafe.Pointer(tdfs)), tdfsSize)

	retTdfs := make([]TrainingDatasetFeature, int(tdfsSize))
	for i, tdf := range tdfsSlice {
		retTdf := TrainingDatasetFeature{
			FeatureID:                int(tdf.feature_id),
			TrainingDataset:          int(tdf.training_dataset),
			FeatureGroupID:           int(tdf.feature_group_id),
			Name:                     C.GoString(&tdf.name[0]),
			Type:                     C.GoString(&tdf.data_type[0]),
			TDJoinID:                 int(tdf.td_join_id),
			IDX:                      int(tdf.idx),
			Label:                    int(tdf.label),
			TransformationFunctionID: int(tdf.transformation_function_id),
			FeatureViewID:            int(tdf.feature_view_id),
		}
		retTdfs[i] = retTdf
	}

	C.free(unsafe.Pointer(tdfs))
	sort.Slice(retTdfs, func(i, j int) bool {
		return retTdfs[i].IDX < retTdfs[j].IDX
	})
	return retTdfs[:], nil
}

func GetFeatureStoreName(fsId int) (string, *DalError) {
	nameBuff := C.malloc(C.size_t(C.FEATURE_STORE_NAME_SIZE))
	defer C.free(nameBuff)

	ret := C.find_feature_store_data(C.int(fsId), (*C.char)(unsafe.Pointer(nameBuff)))

	if ret.http_code != http.StatusOK {
		return "", cToGoRet(&ret)
	}

	return C.GoString((*C.char)(unsafe.Pointer(nameBuff))), nil
}
