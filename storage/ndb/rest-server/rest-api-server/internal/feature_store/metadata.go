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
	"strings"
	"sort"

	"hopsworks.ai/rdrs/internal/dal"
)

type FeatureViewMetadata struct {
	FeatureStoreName       *string `json:"featureStoreName"`
	FeatureStoreId         *int    `json:"featureStoreId"`
	FeatureViewName        *string `json:"featureViewName"`
	FeatureViewId          *int    `json:"featureViewId"`
	FeatureViewVersion     *int    `json:"featureViewVersion"`
	PrefixPrimaryKeyLookup *map[string]*FeatureMetadata
	PrefixFeaturesLookup   *map[string]*FeatureMetadata
	FeatureGroupFeatures   *[]*FeatureGroupFeatures
	NumOfFeatureExPk       *int
	NumOfFeatures          *int
	FeatureIndexLookup     *map[string]int
	FeatureExPkIndexLookup *map[string]int
}

type FeatureGroupFeatures struct {
	FeatureStoreName *string
	FeatureGroupName *string
	Features         *[]*FeatureMetadata
}

type FeatureMetadata struct {
	FeatureStoreName         string
	FeatureGroupName         string
	Id                       int
	Name                     string
	Type                     string
	Index                    int
	Label                    bool
	Prefix                   string
	TransformationFunctionId int
	PrimaryKey				 bool
}

func NewFeatureViewMetadata(
	featureStoreName string,
	featureStoreId int,
	featureViewName string,
	featureViewId int,
	featureViewVersion int,
	features *[]*FeatureMetadata,
) *FeatureViewMetadata {
	pk := make(map[string]*FeatureMetadata)
	prefixColumns := make(map[string]*FeatureMetadata)
	fgFeatures := make(map[string][]*FeatureMetadata)
	for _, feature := range *features {
		prefixFeatureName := feature.Prefix + feature.Name
		if feature.PrimaryKey {
			pk[prefixFeatureName] = feature
		}
		prefixColumns[prefixFeatureName] = feature
		var featureKey = feature.FeatureStoreName + "|" + feature.FeatureGroupName
		fgFeatures[featureKey] = append(fgFeatures[featureKey], feature)
	}

	var fgFeaturesArray = make([]*FeatureGroupFeatures, 0, 0)
	for key, value := range fgFeatures {
		fsName := strings.Split(key, "|")[0]
		fgName := strings.Split(key, "|")[1]
		var featureValue = value
		var fgFeature = FeatureGroupFeatures{&fsName, &fgName, &featureValue}
		fgFeaturesArray = append(fgFeaturesArray, &fgFeature)
	}
	less := func(i, j int) bool {
		return (*features)[i].Index < (*features)[j].Index
	}
	sort.Slice(*features, less)
	featureIndex := make(map[string]int)
	featureExPkIndex := make(map[string]int)
	var nonPkIndex = 0
	var pkIndex = 0
	var numOfPk = len(pk)

	for _, feature := range *features {
		featureIndexKey := GetFeatureIndexKey(feature.FeatureStoreName, feature.FeatureGroupName, feature.Name)
		if feature.PrimaryKey {
			featureIndex[*featureIndexKey] = pkIndex
			pkIndex += 1
		} else {
			featureExPkIndex[*featureIndexKey] = nonPkIndex
			featureIndex[*featureIndexKey] = nonPkIndex + numOfPk
			nonPkIndex += 1
		}
	}

	var numOfFeatureExPk = len(featureExPkIndex)
	var numOfFeature = len(featureIndex)
	var metadata = FeatureViewMetadata{&featureStoreName, &featureStoreId, &featureViewName, &featureViewId, &featureViewVersion, &pk, &prefixColumns, &fgFeaturesArray, &numOfFeatureExPk, &numOfFeature, &featureIndex, &featureExPkIndex}
	return &metadata
}

func GetFeatureIndexKey(fs string, fg string, f string) *string {
	featureIndexKey := fs + "|" + fg + "|" + f
	return &featureIndexKey
}

//TODO cache this medata
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
	// tdJoinID, featureGroupID, prefix, err := dal.GetTrainingDatasetJoinData(fvID)
	// if err != nil {
	// 	return nil, fmt.Errorf("reading training dataset join failed. Error: %s ", err.VerboseError())
	// }

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, fmt.Errorf("reading training dataset feature failed %s ", err.VerboseError())
	}

	features := make([]*FeatureMetadata, len(tdfs), len(tdfs))
	for i, tdf := range tdfs {
		featureGroupName, _, _, err := dal.GetFeatureGroupData(tdf.FeatureGroupID)
		if err != nil {
			return nil, fmt.Errorf("reading feature group failed. Error: %s ", err.VerboseError())
		}
		feature := FeatureMetadata{}
		feature.FeatureStoreName = "" // FIXME: get name from ID 
		feature.FeatureGroupName = featureGroupName
		feature.Id = tdf.FeatureID
		feature.Name = tdf.Name
		feature.Type = tdf.Type
		feature.Index = tdf.IDX
		feature.Label = tdf.Label == 1
		feature.Prefix = "" // FIXME: 
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
