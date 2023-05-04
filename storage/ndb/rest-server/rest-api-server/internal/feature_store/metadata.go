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

type FeatureViewMetadata struct {
	FeatureStoreName     string
	FeatureStoreId       int
	FeatureViewName      string
	FeatureViewId        int
	FeatureViewVersion   int
	PrefixFeaturesLookup map[string]*FeatureMetadata // key: prefix + fName
	FeatureGroupFeatures []*FeatureGroupFeatures
	NumOfFeatures        int
	FeatureIndexLookup   map[string]int // key: fsName + fgName + fName
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

func NewFeatureViewMetadata(
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
		var featureKey = feature.FeatureStoreName + "|" + feature.FeatureGroupName + "|" + strconv.Itoa(feature.FeatureGroupVersion)
		fgFeatures[featureKey] = append(fgFeatures[featureKey], feature)
	}

	var fgFeaturesArray = make([]*FeatureGroupFeatures, 0, 0)
	for key, value := range fgFeatures {
		fsName := strings.Split(key, "|")[0]
		fgName := strings.Split(key, "|")[1]
		fgVersion, _ := strconv.Atoi(strings.Split(key, "|")[2])
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
		featureIndexKey := GetFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.Name)
		featureIndex[*featureIndexKey] = feature.Index
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

func GetFeatureIndexKeyByFeature(feature *FeatureMetadata) *string {
	return GetFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.Name)
}

func GetFeatureIndexKey(fs string, fg string, f string) *string {
	featureIndexKey := fs + "|" + fg + "|" + f
	return &featureIndexKey
}

// TODO cache this medata
func GetFeatureViewMetadata(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, error) {

	fsID, err := dal.GetFeatureStoreID(featureStoreName)
	if err != nil {
		return nil, fmt.Errorf("reading feature store ID failed. Error: %s ", err)
	}

	fvID, err := dal.GetFeatureViewID(fsID, featureViewName, featureViewVersion)
	if err != nil {
		return nil, fmt.Errorf("reading feature view ID failed. Error: %s ", err.VerboseError())
	}

	// FIXME: this should return a list of joinId etc
	joinIdToPrefix := make(map[int]string)
	tdJoins, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		return nil, fmt.Errorf("reading training dataset join failed. Error: %s ", err.VerboseError())
	}
	for _, tdj := range tdJoins {
		joinIdToPrefix[tdj.Id] = tdj.Prefix
	}

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, fmt.Errorf("reading training dataset feature failed %s ", err.VerboseError())
	}

	features := make([]*FeatureMetadata, len(tdfs), len(tdfs))
	fsIdToName := make(map[int]string)

	for i, tdf := range tdfs {
		featureGroupName, _, fsId, fgVersion, err := dal.GetFeatureGroupData(tdf.FeatureGroupID)
		if err != nil {
			return nil, fmt.Errorf("reading feature store failed. Error: %s ", err.VerboseError())
		}
		feature := FeatureMetadata{}
		if featureStoreName, exist := fsIdToName[fsId]; exist {
			feature.FeatureStoreName = featureStoreName
		} else {
			featureStoreName, err = dal.GetFeatureStoreName(fsId)
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
	featureViewMetadata := NewFeatureViewMetadata(
		featureStoreName,
		fsID,
		featureViewName,
		fvID,
		featureViewVersion,
		&features,
	)
	return featureViewMetadata, nil
}
