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
	"strconv"
	"strings"
	"time"

	"github.com/patrickmn/go-cache"

	"hopsworks.ai/rdrs/internal/dal"
	"hopsworks.ai/rdrs/internal/log"
)

type FeatureViewMetadata struct {
	FeatureStoreName     string
	FeatureStoreId       int
	FeatureViewName      string
	FeatureViewId        int
	FeatureViewVersion   int
	PrefixFeaturesLookup map[string]*FeatureMetadata // key: prefix + fName
	FeatureGroupFeatures []*FeatureGroupFeatures
	FeatureStoreNames    []*string
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
	var fsNames = []*string{}
	var fsNameMap = make(map[string]bool)
	for _, fgf := range fgFeaturesArray {
		var fgName = fgf.FeatureStoreName
		if !fsNameMap[fgName] {
			fsNames = append(fsNames, &fgName)
			fsNameMap[fgName] = true
		}
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

func GetFeatureIndexKeyByFgIndexKey(fgKey string, featureName string) string {
	return fmt.Sprintf("%s|%s", fgKey, featureName)
}

func getFeatureGroupIndexKey(fs string, fg string, fgVersion int) *string {
	featureIndexKey := fmt.Sprintf("%s|%s|%d", fs, fg, fgVersion)
	return &featureIndexKey
}

func getFeatureIndexKey(fs string, fg string, fgVersion int, f string) *string {
	var featureIndexKey = GetFeatureIndexKeyByFgIndexKey(*getFeatureGroupIndexKey(fs, fg, fgVersion), f)
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
		if strings.Contains(err.Error(), "Not Found") {
			return nil, FS_NOT_EXIST
		}
		return nil, (*FS_NOT_EXIST).NewMessage(err.Error())
	}

	fvID, err := dal.GetFeatureViewID(fsID, featureViewName, featureViewVersion)
	if err != nil {
		if strings.Contains(err.Error(), "Not Found") {
			return nil, FV_NOT_EXIST
		}
		return nil, (*&FV_READ_FAIL).NewMessage(err.VerboseError())
	}

	joinIdToPrefix := make(map[int]string)
	tdJoins, err := dal.GetTrainingDatasetJoinData(fvID)
	if err != nil {
		return nil, (*TD_JOIN_READ_FAIL).NewMessage(err.VerboseError())
	}
	for _, tdj := range tdJoins {
		joinIdToPrefix[tdj.Id] = tdj.Prefix
	}

	tdfs, err := dal.GetTrainingDatasetFeature(fvID)
	if err != nil {
		return nil, (*TD_FEATURE_READ_FAIL).NewMessage(err.VerboseError())
	}

	features := make([]*FeatureMetadata, len(tdfs))
	fsIdToName := make(map[int]string)

	for i, tdf := range tdfs {
		featureGroupName, _, fsId, fgVersion, err := dal.GetFeatureGroupData(tdf.FeatureGroupID)
		if err != nil {
			if strings.Contains(err.Error(), "Not Found") {
				return nil, FG_NOT_EXIST
			}
			return nil, (*FG_READ_FAIL).NewMessage(err.VerboseError())
		}
		feature := FeatureMetadata{}
		if featureStoreName, exist := fsIdToName[fsId]; exist {
			feature.FeatureStoreName = featureStoreName
		} else {
			featureStoreName, err = dal.GetFeatureStoreName(fsId)
			if err != nil {
				if strings.Contains(err.Error(), "Not Found") {
					return nil, FS_NOT_EXIST
				}
				return nil, (*FS_READ_FAIL).NewMessage(err.VerboseError())
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
