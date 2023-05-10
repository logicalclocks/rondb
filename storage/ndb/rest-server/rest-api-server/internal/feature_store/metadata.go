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

package feature_store

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"hopsworks.ai/rdrs/internal/dal"
)

const (
	FV_NOT_EXIST         = "Feature view does not exist."
	FS_NOT_EXIST         = "Feature store does not exist."
	FG_NOT_EXIST         = "Feature group does not exist."
	FG_READ_FAIL         = "Reading feature group failed."
	FS_READ_FAIL         = "Reading feature store failed."
	FV_READ_FAIL         = "Reading feature view failed."
	TD_JOIN_READ_FAIL    = "Reading training dataset join failed."
	TD_FEATURE_READ_FAIL = "Reading training dataset feature failed."
)

type FeatureViewMetadata struct {
	FeatureStoreName     string
	FeatureStoreId       int
	FeatureViewName      string
	FeatureViewId        int
	FeatureViewVersion   int
	PrefixFeaturesLookup map[string]*FeatureMetadata // key: prefix + fName
	FeatureGroupFeatures []*FeatureGroupFeatures
	NumOfFeatures        int
	FeatureIndexLookup   map[string]int // key: fsName + fgName + fgVersion + fName
}

type FeatureGroupFeatures struct {
	FeatureStoreName    string
	FeatureGroupName    string
	FeatureGroupVersion int
	Features            []*FeatureMetadata
}

type FeatureMetadata struct {
	FeatureStoreName         string
	FeatureGroupName         string
	FeatureGroupVersion      int
	Id                       int
	Name                     string
	Type                     string
	Index                    int
	Label                    bool
	Prefix                   string
	TransformationFunctionId int
}

func newFeatureViewMetadata(
	featureStoreName string,
	featureStoreId int,
	featureViewName string,
	featureViewId int,
	featureViewVersion int,
	features *[]*FeatureMetadata,
) *FeatureViewMetadata {
	prefixColumns := make(map[string]*FeatureMetadata)
	fgFeatures := make(map[string][]*FeatureMetadata)
	for _, feature := range *features {
		prefixFeatureName := feature.Prefix + feature.Name
		prefixColumns[prefixFeatureName] = feature
		var featureKey = GetFeatureGroupKeyByFeature(feature)
		fgFeatures[featureKey] = append(fgFeatures[featureKey], feature)
	}

	var fgFeaturesArray = make([]*FeatureGroupFeatures, 0)
	for key, value := range fgFeatures {
		var fsName, fgName, fgVersion = GetFeatureGroupDetailByKey(key)
		var featureValue = value
		var fgFeature = FeatureGroupFeatures{fsName, fgName, fgVersion, featureValue}
		fgFeaturesArray = append(fgFeaturesArray, &fgFeature)
	}
	less := func(i, j int) bool {
		return (*features)[i].Index < (*features)[j].Index
	}
	sort.Slice(*features, less)
	featureIndex := make(map[string]int)

	for _, feature := range *features {
		featureIndexKey := GetFeatureIndexKeyByFeature(feature)
		featureIndex[featureIndexKey] = feature.Index
	}

	var numOfFeature = len(featureIndex)
	var metadata = FeatureViewMetadata{
		featureStoreName,
		featureStoreId,
		featureViewName,
		featureViewId,
		featureViewVersion,
		prefixColumns,
		fgFeaturesArray,
		numOfFeature,
		featureIndex}
	return &metadata
}

func GetFeatureGroupDetailByKey(key string) (string, string, int) {
	var splitedKey = strings.Split(key, "|")
	fsName := splitedKey[0]
	fgName := splitedKey[1]
	fgVersion, err := strconv.Atoi(splitedKey[2])
	if err != nil {
		panic(fmt.Sprintf("Invalid feature group version '%s' in feature group key.", splitedKey[2]))
	}
	return fsName, fgName, fgVersion
}

func GetFeatureGroupKeyByFeature(feature *FeatureMetadata) string {
	return *getFeatureGroupIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.FeatureGroupVersion)
}

func GetFeatureGroupKeyByTDFeature(feature *FeatureGroupFeatures) string {
	return *getFeatureGroupIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.FeatureGroupVersion)
}

func GetFeatureIndexKeyByFeature(feature *FeatureMetadata) string {
	return *getFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.FeatureGroupVersion, feature.Name)
}

func getFeatureGroupIndexKey(fs string, fg string, fgVersion int) *string {
	featureIndexKey := fmt.Sprintf("%s|%s|%d", fs, fg, fgVersion)
	return &featureIndexKey
}

func getFeatureIndexKey(fs string, fg string, fgVersion int, f string) *string {
	featureIndexKey := fmt.Sprintf("%s|%s", *getFeatureGroupIndexKey(fs, fg, fgVersion), f)
	return &featureIndexKey
}

// TODO cache this medata
func GetFeatureViewMetadata(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, error) {

	fsID, err := dal.GetFeatureStoreID(featureStoreName)
	if err != nil {
		if strings.Contains(err.Error(), "Not Found") {
			return nil, fmt.Errorf(FS_NOT_EXIST)
		}
		return nil, fmt.Errorf("%s Error: %s ", FS_READ_FAIL, err)
	}

	fvID, err := dal.GetFeatureViewID(fsID, featureViewName, featureViewVersion)
	if err != nil {
		if strings.Contains(err.Error(), "Not Found") {
			return nil, fmt.Errorf(FV_NOT_EXIST)
		}
		return nil, fmt.Errorf("%s Error: %s ", FV_READ_FAIL, err)
	}

	joinIdToPrefix := make(map[int]string)
	tdJoins, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		return nil, fmt.Errorf("%s Error: %s ", TD_JOIN_READ_FAIL, err.VerboseError())
	}
	for _, tdj := range tdJoins {
		joinIdToPrefix[tdj.Id] = tdj.Prefix
	}

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, fmt.Errorf("%s Error: %s ", TD_FEATURE_READ_FAIL, err.VerboseError())
	}

	features := make([]*FeatureMetadata, len(tdfs))
	fsIdToName := make(map[int]string)

	for i, tdf := range tdfs {
		featureGroupName, _, fsId, fgVersion, err := dal.GetFeatureGroupData(tdf.FeatureGroupID)
		if err != nil {
			if strings.Contains(err.Error(), "Not Found") {
				return nil, fmt.Errorf(FG_NOT_EXIST)
			}
			return nil, fmt.Errorf("%s Error: %s ", FG_READ_FAIL, err)
		}
		feature := FeatureMetadata{}
		if featureStoreName, exist := fsIdToName[fsId]; exist {
			feature.FeatureStoreName = featureStoreName
		} else {
			featureStoreName, err = dal.GetFeatureStoreName(fsId)
			if err != nil {
				if strings.Contains(err.Error(), "Not Found") {
					return nil, fmt.Errorf(FS_NOT_EXIST)
				}
				return nil, fmt.Errorf("%s Error: %s ", FS_READ_FAIL, err)
			}
			fsIdToName[fsId] = featureStoreName
			feature.FeatureStoreName = featureStoreName
		}
		feature.FeatureGroupName = featureGroupName
		feature.FeatureGroupVersion = fgVersion
		feature.Id = tdf.FeatureID
		feature.Name = tdf.Name
		feature.Type = tdf.Type
		feature.Index = tdf.IDX
		feature.Label = tdf.Label == 1
		feature.Prefix = joinIdToPrefix[tdf.TDJoinID]
		feature.TransformationFunctionId = tdf.TransformationFunctionID
		features[i] = &feature
	}
	featureViewMetadata := newFeatureViewMetadata(
		featureStoreName,
		fsID,
		featureViewName,
		fvID,
		featureViewVersion,
		&features,
	)
	return featureViewMetadata, nil
}
