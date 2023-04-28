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
)

type FeatureStoreMetadata struct {
	FeatureStoreID            int    //       # query 1
	FeatureViewID             int    //      # query 1 -> 2
	FeatureStoreName          string //,     # REST path param -> Cache Key
	FeatureViewName           string //      # REST path param -> Cache Key
	FeatureViewVersion        int    //   # REST path param -> Cache Key
	TDJoinID                  int
	TDJoinPrefix              string
	FeatureGroupName          string
	FeatureGroupOnlineEnabled bool
	TrainingDatasetFeatures   []TrainingDatasetFeature
}

type TrainingDatasetFeature struct {
	FeatureID                int
	TrainingDataset          int
	featureGroupID           int
	Name                     string
	Type                     string
	TDJoinID                 int
	IDX                      int
	Label                    int
	TransformationFunctionID int
	FeatureViewID            int
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

func GetTrainingDatasetJoinData(featureViewID int) (int, int, string, *DalError) {
	prefixBuff := C.malloc(C.size_t(C.TRAINING_DATASET_JOIN_PREFIX_SIZE))
	defer C.free(prefixBuff)

	var tdJoinID C.int
	tdJoinIDPtr := (*C.int)(unsafe.Pointer(&tdJoinID))

	var featureGroupID C.int
	featureGroupIDPtr := (*C.int)(unsafe.Pointer(&featureGroupID))

	ret := C.find_training_dataset_join_data(C.int(featureViewID),
		tdJoinIDPtr,
		featureGroupIDPtr,
		(*C.char)(unsafe.Pointer(prefixBuff)))

	if ret.http_code != http.StatusOK {
		return 0, 0, "", cToGoRet(&ret)
	}

	return int(tdJoinID), int(featureGroupID), C.GoString((*C.char)(unsafe.Pointer(prefixBuff))), nil
}

func GetFeatureGroupData(featureGroupID int) (string, bool, int, *DalError) {
	nameBuff := C.malloc(C.size_t(C.FEATURE_GROUP_NAME_SIZE))
	defer C.free(nameBuff)

	var onlineEnabled C.int
	onlineEnabledPtr := (*C.int)(unsafe.Pointer(&onlineEnabled))

	var featureStoreID C.int
	featureStoreIDPtr := (*C.int)(unsafe.Pointer(&featureStoreID))

	ret := C.find_feature_group_data(C.int(featureGroupID),
		(*C.char)(unsafe.Pointer(nameBuff)), onlineEnabledPtr, featureStoreIDPtr)

	if ret.http_code != http.StatusOK {
		return "", false, 0, cToGoRet(&ret)
	}

	return C.GoString((*C.char)(unsafe.Pointer(nameBuff))), int(onlineEnabled) != 0, int(featureGroupID), nil
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
			featureGroupID:           int(tdf.feature_group_id),
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
