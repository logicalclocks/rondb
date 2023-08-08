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
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/log"
)

const ERROR_NOT_FOUND = "Not Found"

type FeatureViewMetadata struct {
	FeatureStoreName     string
	FeatureStoreId       int
	FeatureViewName      string
	FeatureViewId        int
	FeatureViewVersion   int
	PrefixFeaturesLookup map[string]*FeatureMetadata // key: prefix + fName, label are excluded
	FeatureGroupFeatures []*FeatureGroupFeatures     // label are excluded
	FeatureStoreNames    []*string                   // List of all feature store used by feature view including shared feature store
	NumOfFeatures        int
	FeatureIndexLookup   map[string]int             // key: joinIndex + fgId + fName, label are excluded. joinIndex is needed because of self-join
	PrimaryKeyMap        map[string]*dal.ServingKey // key: join index + feature name
	PrefixPrimaryKeyMap  map[string]string          // key: prefix(collision corrected) + fName, value: feature name in feature group
}

type FeatureGroupFeatures struct {
	FeatureStoreName    string
	FeatureGroupName    string
	FeatureGroupVersion int
	FeatureGroupId      int
	JoinIndex           int
	Features            []*FeatureMetadata
	PrimaryKeyMap       []*dal.ServingKey
}

type FeatureMetadata struct {
	FeatureStoreName         string
	FeatureGroupName         string
	FeatureGroupVersion      int
	FeatureGroupId           int
	Id                       int
	Name                     string
	Type                     string
	Index                    int
	Label                    bool
	Prefix                   string
	TransformationFunctionId int
	JoinIndex                int
}

func newFeatureViewMetadata(
	featureStoreName string,
	featureStoreId int,
	featureViewName string,
	featureViewId int,
	featureViewVersion int,
	features *[]*FeatureMetadata,
	servingKeys *[]dal.ServingKey,
) *FeatureViewMetadata {
	var prefixPrimaryKeyMap = make(map[string]string)
	var primaryKeyMap = make(map[string]*dal.ServingKey)
	var fgPrimaryKeyMap = make(map[string][]*dal.ServingKey)
	for _, key := range *servingKeys {
		prefixPrimaryKeyMap[key.Prefix+key.FeatureName] = key.FeatureName
		var newKey = key
		primaryKeyMap[GetServingKey(key.JoinIndex, key.FeatureName)] = &newKey
		var fgKey = getFeatureGroupServingKey(key.JoinIndex, key.FeatureGroupId)
		fgPrimaryKeyMap[fgKey] = append(fgPrimaryKeyMap[fgKey], &newKey)
	}

	prefixColumns := make(map[string]*FeatureMetadata)
	fgFeatures := make(map[string][]*FeatureMetadata)
	for _, feature := range *features {
		if feature.Label {
			continue
		}
		prefixFeatureName := feature.Prefix + feature.Name
		prefixColumns[prefixFeatureName] = feature
		var featureKey = fmt.Sprintf("%d|%d", feature.JoinIndex, feature.FeatureGroupId)
		fgFeatures[featureKey] = append(fgFeatures[featureKey], feature)
	}

	var fgFeaturesArray = make([]*FeatureGroupFeatures, 0)

	for _, value := range fgFeatures {
		var featureValue = value
		var feature = featureValue[0]
		var fgFeature = FeatureGroupFeatures{}
		fgFeature.FeatureStoreName = feature.FeatureStoreName
		fgFeature.FeatureGroupName = feature.FeatureGroupName
		fgFeature.FeatureGroupVersion = feature.FeatureGroupVersion
		fgFeature.FeatureGroupId = feature.FeatureGroupId
		fgFeature.Features = featureValue
		var fgPk, ok = fgPrimaryKeyMap[getFeatureGroupServingKey(feature.JoinIndex, feature.FeatureGroupId)]
		if ok {
			fgFeature.PrimaryKeyMap = fgPk
		}
		fgFeature.JoinIndex = feature.JoinIndex
		fgFeaturesArray = append(fgFeaturesArray, &fgFeature)
	}
	less := func(i, j int) bool {
		return (*features)[i].Index < (*features)[j].Index
	}
	sort.Slice(*features, less)
	featureIndex := make(map[string]int)
	var featureCount = 0
	for _, feature := range *features {
		if feature.Label {
			continue
		}
		featureIndexKey := GetFeatureIndexKeyByFeature(feature)
		featureIndex[featureIndexKey] = featureCount
		featureCount++
	}
	var fsNames = []*string{}
	var fsNameMap = make(map[string]bool)
	for _, fgf := range fgFeaturesArray {
		var fgName = fgf.FeatureStoreName
		if !fsNameMap[fgName] {
			fsNames = append(fsNames, &fgName)
			fsNameMap[fgName] = true
		}
	}
	if !fsNameMap[featureStoreName] {
		fsNames = append(fsNames, &featureStoreName)
	}
	var numOfFeature = len(featureIndex)
	var metadata = FeatureViewMetadata{}
	metadata.FeatureStoreName = featureStoreName
	metadata.FeatureStoreId = featureStoreId
	metadata.FeatureViewName = featureViewName
	metadata.FeatureViewId = featureViewId
	metadata.FeatureViewVersion = featureViewVersion
	metadata.PrefixFeaturesLookup = prefixColumns
	metadata.FeatureGroupFeatures = fgFeaturesArray
	metadata.NumOfFeatures = numOfFeature
	metadata.FeatureIndexLookup = featureIndex
	metadata.FeatureStoreNames = fsNames
	metadata.PrimaryKeyMap = primaryKeyMap
	metadata.PrefixPrimaryKeyMap = prefixPrimaryKeyMap
	return &metadata
}

func getFeatureGroupServingKey(joinIndex int, featureGroupId int) string {
	return fmt.Sprintf("%d|%d", joinIndex, featureGroupId)
}

func GetServingKey(joinIndex int, featureName string) string {
	return fmt.Sprintf("%d|%s", joinIndex, featureName)
}

func GetFeatureGroupKeyByFeature(feature *FeatureMetadata) string {
	return *getFeatureGroupIndexKey(feature.JoinIndex, feature.FeatureGroupId)
}

func GetFeatureGroupKeyByTDFeature(feature *FeatureGroupFeatures) string {
	return *getFeatureGroupIndexKey(feature.JoinIndex, feature.FeatureGroupId)
}

func GetFeatureIndexKeyByFeature(feature *FeatureMetadata) string {
	return *getFeatureIndexKey(feature.JoinIndex, feature.FeatureGroupId, feature.Name)
}

func GetFeatureIndexKeyByFgIndexKey(fgKey string, featureName string) string {
	return fmt.Sprintf("%s|%s", fgKey, featureName)
}

func getFeatureGroupIndexKey(joinIndex int, fgId int) *string {
	var indexKey = fmt.Sprintf("%d|%d", joinIndex, fgId)
	return &indexKey
}

func getFeatureIndexKey(joinIndex int, fgId int, f string) *string {
	var featureIndexKey = GetFeatureIndexKeyByFgIndexKey(*getFeatureGroupIndexKey(joinIndex, fgId), f)
	return &featureIndexKey
}

type FeatureViewMetaDataCache struct {
	metadataCache cache.Cache
}

func NewFeatureViewMetaDataCache() *FeatureViewMetaDataCache {
	var c = cache.New(15*time.Minute, 15*time.Minute)
	return &FeatureViewMetaDataCache{*c}
}

func (fvmeta *FeatureViewMetaDataCache) Get(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, *RestErrorCode) {
	var fvCacheKey = getFeatureViewCacheKey(featureStoreName, featureViewName, featureViewVersion)
	var metadataInf, exist = fvmeta.metadataCache.Get(fvCacheKey)
	if !exist {
		var metadata, err = GetFeatureViewMetadata(featureStoreName, featureViewName, featureViewVersion)
		if err != nil {
			return nil, err
		} else {
			fvmeta.metadataCache.SetDefault(fvCacheKey, metadata)
			if log.IsDebug() {
				metadataJson, _ := json.MarshalIndent(metadata, "", "  ")
				log.Debugf("Feature store metadata is %s", metadataJson)
			}
			return metadata, nil
		}
	} else {
		var metadata, ok = metadataInf.(*FeatureViewMetadata)
		if !ok {
			return nil, FETCH_METADATA_FROM_CACHE_FAIL
		}
		return metadata, nil
	}
}

func getFeatureViewCacheKey(featureStoreName, featureViewName string, featureViewVersion int) string {
	return fmt.Sprintf("%s|%s|%d", featureStoreName, featureViewName, featureViewVersion)
}

func GetFeatureViewMetadata(featureStoreName, featureViewName string, featureViewVersion int) (*FeatureViewMetadata, *RestErrorCode) {
	fsID, err := dal.GetFeatureStoreID(featureStoreName)
	if err != nil {
		if strings.Contains(err.Error(), ERROR_NOT_FOUND) {
			return nil, FS_NOT_EXIST
		}
		return nil, FS_NOT_EXIST.NewMessage(err.Error())
	}

	fvID, err := dal.GetFeatureViewID(fsID, featureViewName, featureViewVersion)
	if err != nil {
		if strings.Contains(err.Error(), ERROR_NOT_FOUND) {
			return nil, FV_NOT_EXIST
		}
		return nil, FV_READ_FAIL.NewMessage(err.VerboseError())
	}

	joinIdToJoin := make(map[int]dal.TrainingDatasetJoin)
	tdJoins, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		if strings.Contains(err.Error(), ERROR_NOT_FOUND) {
			return nil, FG_NOT_EXIST.NewMessage("Feature view may contain deleted feature groups.")
		}
		return nil, TD_JOIN_READ_FAIL.NewMessage(err.VerboseError())
	}
	for _, tdj := range tdJoins {
		joinIdToJoin[tdj.Id] = tdj
	}

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, TD_FEATURE_READ_FAIL.NewMessage(err.VerboseError())
	}

	features := make([]*FeatureMetadata, len(tdfs))
	fsIdToName := make(map[int]string)
	var fgCache = make(map[int]*dal.FeatureGroup)

	for i, tdf := range tdfs {
		if tdf.FeatureGroupID == 0 {
			// If a feature group is deleted, feature group id will be set to null in the db.
			return nil, FG_NOT_EXIST.NewMessage(
				fmt.Sprintf("Cannot get the feature group of feature `%s`. Check if the feature group still exists.", tdf.Name))
		}
		var featureGroup *dal.FeatureGroup
		if fg, ok := fgCache[tdf.FeatureGroupID]; ok {
			featureGroup = fg
		} else {
			featureGroup, err = dal.GetFeatureGroupData(tdf.FeatureGroupID)
			if err != nil {
				if strings.Contains(err.Error(), ERROR_NOT_FOUND) {
					return nil, FG_NOT_EXIST
				}
				return nil, FG_READ_FAIL.NewMessage(err.VerboseError())
			}
			fgCache[tdf.FeatureGroupID] = featureGroup
		}
		feature := FeatureMetadata{}
		if featureStoreName, exist := fsIdToName[featureGroup.FeatureStoreId]; exist {
			feature.FeatureStoreName = featureStoreName
		} else {
			featureStoreName, err = dal.GetFeatureStoreName(featureGroup.FeatureStoreId)
			if err != nil {
				if strings.Contains(err.Error(), ERROR_NOT_FOUND) {
					return nil, FS_NOT_EXIST
				}
				return nil, FS_READ_FAIL.NewMessage(err.VerboseError())
			}
			fsIdToName[featureGroup.FeatureStoreId] = featureStoreName
			feature.FeatureStoreName = featureStoreName
		}
		feature.FeatureGroupName = featureGroup.Name
		feature.FeatureGroupVersion = featureGroup.Version
		feature.FeatureGroupId = tdf.FeatureGroupID
		feature.Id = tdf.FeatureID
		feature.Name = tdf.Name
		feature.Type = tdf.Type
		feature.Index = tdf.IDX
		feature.Label = tdf.Label == 1
		feature.Prefix = joinIdToJoin[tdf.TDJoinID].Prefix
		feature.JoinIndex = joinIdToJoin[tdf.TDJoinID].Index
		feature.TransformationFunctionId = tdf.TransformationFunctionID
		features[i] = &feature
	}
	var servingKeys, err1 = dal.GetServingKeys(fvID)
	if err1 != nil {
		return nil, FV_READ_FAIL.NewMessage("Failed to read serving keys.")
	}
	featureViewMetadata := newFeatureViewMetadata(
		featureStoreName,
		fsID,
		featureViewName,
		fvID,
		featureViewVersion,
		&features,
		&servingKeys,
	)
	return featureViewMetadata, nil
}
